package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** One line of an {@link InventoryEvent}. */
@DefaultCoder(SerializableCoder.class)
public record InventoryLine(
    Integer lineNumber, String sku, Integer quantity, String status, String locationId)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
