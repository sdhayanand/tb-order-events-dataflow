package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Builds the {@code events-dlq} message (ARCHITECTURE.md section 3.4): the original payload as
 * {@code data}, the original attributes, plus {@code dlqReason}, {@code dlqStage} and
 * {@code originalTopic}. Attribute values are capped well below Pub/Sub's 1024-byte limit.
 */
public class DeadLetterToPubsubMessageFn extends DoFn<DeadLetter, PubsubMessage> {

  private static final long serialVersionUID = 1L;
  private static final int MAX_ATTRIBUTE_LENGTH = 900;

  @ProcessElement
  public void processElement(@Element DeadLetter d, OutputReceiver<PubsubMessage> out) {
    Map<String, String> attributes = new HashMap<>();
    if (d.attributes() != null) {
      attributes.putAll(d.attributes());
    }
    attributes.put("dlqReason", cap(d.reason()));
    attributes.put("dlqStage", cap(d.stage()));
    attributes.put("originalTopic", cap(d.originalTopic()));
    if (d.messageId() != null) {
      attributes.put("originalMessageId", d.messageId());
    }
    if (d.failedAt() != null) {
      attributes.put("dlqFailedAt", d.failedAt());
    }
    byte[] payload = (d.payload() == null ? "" : d.payload()).getBytes(StandardCharsets.UTF_8);
    out.output(new PubsubMessage(payload, attributes));
  }

  private static String cap(String s) {
    if (s == null) {
      return "";
    }
    return s.length() <= MAX_ATTRIBUTE_LENGTH ? s : s.substring(0, MAX_ATTRIBUTE_LENGTH);
  }
}
