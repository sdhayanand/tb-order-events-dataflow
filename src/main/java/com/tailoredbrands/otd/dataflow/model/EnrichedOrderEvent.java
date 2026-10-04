package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** {@link OrderEvent} after store-reference enrichment, ready to be flattened into BigQuery rows. */
@DefaultCoder(SerializableCoder.class)
public record EnrichedOrderEvent(
    OrderEvent event,
    String messageId,
    String publishTime,
    String storeName,
    String region,
    String storeTimezone)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
