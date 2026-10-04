package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * A successfully parsed and validated event together with the Pub/Sub delivery metadata that we
 * want to keep in BigQuery (message id for dedup, publish time for latency analysis, attributes
 * for lineage).
 *
 * @param <T> the parsed payload type
 */
@DefaultCoder(SerializableCoder.class)
public record Received<T extends Serializable>(
    T payload, String messageId, String publishTime, HashMap<String, String> attributes)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public static <T extends Serializable> Received<T> of(
      T payload, String messageId, String publishTime, Map<String, String> attributes) {
    return new Received<>(
        payload,
        messageId,
        publishTime,
        attributes == null ? new HashMap<>() : new HashMap<>(attributes));
  }
}
