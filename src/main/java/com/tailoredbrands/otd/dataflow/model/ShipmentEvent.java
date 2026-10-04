package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** {@code ShipmentEvent} published on {@code shipments-v1} (ARCHITECTURE.md section 3.3). */
@DefaultCoder(SerializableCoder.class)
public record ShipmentEvent(
    String eventId,
    String eventType,
    String eventTime,
    String schemaVersion,
    String source,
    String correlationId,
    String orderId,
    String trackingNumber,
    String carrier,
    String status,
    String statusTime,
    String location)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
