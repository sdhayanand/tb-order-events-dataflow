package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** One {@code <Line>} of a {@link LegacyOrder}. */
@DefaultCoder(SerializableCoder.class)
public record LegacyLine(Integer lineNbr, String sku, Integer qty, Double price, String fulfillType)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public double lineAmount() {
    return (qty == null ? 0 : qty) * (price == null ? 0.0 : price);
  }
}
