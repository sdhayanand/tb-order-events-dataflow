package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Canonical {@code OrderEvent} envelope published on {@code orders-v1} (ARCHITECTURE.md section 3.1).
 * Timestamps are kept as RFC-3339 strings exactly as they appear on the wire; they are validated by
 * {@link com.tailoredbrands.otd.dataflow.transforms.OrderEventValidator}.
 */
@DefaultCoder(SerializableCoder.class)
public record OrderEvent(
    String eventId,
    String eventType,
    String eventTime,
    String schemaVersion,
    String source,
    String correlationId,
    String legacyMessageId,
    Order order)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public boolean orderCreated() {
    return EventTypes.ORDER_CREATED.equals(eventType);
  }
}
