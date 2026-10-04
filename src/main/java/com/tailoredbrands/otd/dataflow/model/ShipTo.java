package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Ship-to address for ECOM / ship-to-home orders. Persisted as a JSON string in BigQuery. */
@DefaultCoder(SerializableCoder.class)
public record ShipTo(
    String name,
    String line1,
    String line2,
    String city,
    String state,
    String postalCode,
    String country)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
