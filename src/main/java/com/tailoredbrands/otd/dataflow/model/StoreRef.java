package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Store reference data (CSV {@code store_id,store_name,region,timezone}) used as a side input. */
@DefaultCoder(SerializableCoder.class)
public record StoreRef(String storeId, String storeName, String region, String timezone)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
