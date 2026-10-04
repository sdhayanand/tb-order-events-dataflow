package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Output of {@link com.tailoredbrands.otd.dataflow.transforms.StoreMetricsCombineFn} per store and window. */
@DefaultCoder(SerializableCoder.class)
public record StoreMetrics(long orders, double revenue, long alterationLines, long rentalOrders)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
