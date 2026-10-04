package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** One order line. {@code alteration} is only present when {@code fulfillmentType == ALTERATION}. */
@DefaultCoder(SerializableCoder.class)
public record OrderLine(
    Integer lineNumber,
    String sku,
    Integer quantity,
    Double unitPrice,
    String fulfillmentType,
    Alteration alteration)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Not {@code isAlteration()}: that would be a Jackson getter clashing with the {@code alteration} component. */
  public boolean needsAlteration() {
    return FulfillmentTypes.ALTERATION.equals(fulfillmentType);
  }

  public double lineAmount() {
    int qty = quantity == null ? 0 : quantity;
    double price = unitPrice == null ? 0.0 : unitPrice;
    return qty * price;
  }
}
