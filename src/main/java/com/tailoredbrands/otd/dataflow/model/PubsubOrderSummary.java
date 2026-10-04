package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** The few {@code otd.order_events} columns the reconciliation needs, one per ORDER_CREATED event. */
@DefaultCoder(SerializableCoder.class)
public record PubsubOrderSummary(
    String orderId, String storeId, String eventTime, double totalAmount, long lineCount)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
