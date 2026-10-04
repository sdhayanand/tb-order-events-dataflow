package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** {@code InventoryEvent} published on {@code inventory-v1} (ARCHITECTURE.md section 3.2). */
@DefaultCoder(SerializableCoder.class)
public record InventoryEvent(
    String eventId,
    String eventType,
    String eventTime,
    String schemaVersion,
    String source,
    String correlationId,
    String orderId,
    String storeId,
    List<InventoryLine> lines)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public List<InventoryLine> safeLines() {
    return lines == null ? List.of() : lines;
  }
}
