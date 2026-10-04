package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Alteration work-order details attached to an {@link OrderLine}. */
@DefaultCoder(SerializableCoder.class)
public record Alteration(String type, Double measurementInches, String tailorShopId)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
