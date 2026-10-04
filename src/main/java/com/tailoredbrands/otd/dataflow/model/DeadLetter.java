package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Pipeline-level dead letter (ARCHITECTURE.md section 3.4). Carries the original payload and
 * attributes untouched plus {@code reason} / {@code stage} / {@code originalTopic}.
 */
@DefaultCoder(SerializableCoder.class)
public record DeadLetter(
    String originalTopic,
    String stage,
    String reason,
    String payload,
    HashMap<String, String> attributes,
    String messageId,
    String failedAt)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String STAGE_PARSE = "PARSE";
  public static final String STAGE_VALIDATE = "VALIDATE";
  public static final String STAGE_BIGQUERY_WRITE = "BIGQUERY_WRITE";

  public static DeadLetter of(
      String originalTopic,
      String stage,
      String reason,
      String payload,
      Map<String, String> attributes,
      String messageId,
      String failedAt) {
    return new DeadLetter(
        originalTopic,
        stage,
        reason,
        payload,
        attributes == null ? new HashMap<>() : new HashMap<>(attributes),
        messageId,
        failedAt);
  }
}
