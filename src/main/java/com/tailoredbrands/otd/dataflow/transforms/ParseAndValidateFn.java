package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.util.Json;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a {@link PubsubMessage} payload with Jackson into {@code T}, runs a {@link Validator} and
 * emits either {@code Received<T>} on the main output or a {@link DeadLetter} on the side output
 * tag. Nothing is ever dropped silently: every message ends up in exactly one of the two outputs.
 *
 * <p>Counters {@code <stream>/parsed} and {@code <stream>/invalid} are published as Beam metrics
 * (visible in the Dataflow UI and Cloud Monitoring as custom metrics).
 *
 * @param <T> event type (must be Java-serializable and a Jackson-deserializable record)
 */
public class ParseAndValidateFn<T extends Serializable> extends DoFn<PubsubMessage, Received<T>> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ParseAndValidateFn.class);
  private static final int MAX_REASON_LENGTH = 1000;

  private final Class<T> type;
  private final Validator<T> validator;
  private final String streamName;
  private final String originalTopic;
  private final TupleTag<DeadLetter> deadLetterTag;
  private final Counter parsed;
  private final Counter invalid;

  public ParseAndValidateFn(
      Class<T> type,
      Validator<T> validator,
      String streamName,
      String originalTopic,
      TupleTag<DeadLetter> deadLetterTag) {
    this.type = type;
    this.validator = validator;
    this.streamName = streamName;
    this.originalTopic = originalTopic;
    this.deadLetterTag = deadLetterTag;
    this.parsed = Metrics.counter("otd." + streamName, "parsed");
    this.invalid = Metrics.counter("otd." + streamName, "invalid");
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    PubsubMessage message = c.element();
    byte[] payloadBytes = message.getPayload() == null ? new byte[0] : message.getPayload();
    String payload = new String(payloadBytes, StandardCharsets.UTF_8);
    Map<String, String> attributes = message.getAttributeMap();
    String messageId = message.getMessageId();
    String publishTime = Times.toIso(c.timestamp());

    T event;
    try {
      event = Json.read(payloadBytes, type);
    } catch (Exception e) {
      invalid.inc();
      LOG.warn("[{}] unparseable message {}: {}", streamName, messageId, e.getMessage());
      c.output(
          deadLetterTag,
          DeadLetter.of(
              originalTopic,
              DeadLetter.STAGE_PARSE,
              truncate("JSON parse error: " + e.getMessage()),
              payload,
              attributes,
              messageId,
              Times.nowIso()));
      return;
    }
    if (event == null) {
      invalid.inc();
      c.output(
          deadLetterTag,
          DeadLetter.of(
              originalTopic,
              DeadLetter.STAGE_PARSE,
              "empty payload",
              payload,
              attributes,
              messageId,
              Times.nowIso()));
      return;
    }

    List<String> problems = validator.validate(event);
    if (!problems.isEmpty()) {
      invalid.inc();
      LOG.warn("[{}] invalid message {}: {}", streamName, messageId, problems);
      c.output(
          deadLetterTag,
          DeadLetter.of(
              originalTopic,
              DeadLetter.STAGE_VALIDATE,
              truncate(String.join("; ", problems)),
              payload,
              attributes,
              messageId,
              Times.nowIso()));
      return;
    }

    parsed.inc();
    c.output(Received.of(event, messageId, publishTime, attributes));
  }

  private static String truncate(String s) {
    return s.length() <= MAX_REASON_LENGTH ? s : s.substring(0, MAX_REASON_LENGTH) + "...";
  }
}
