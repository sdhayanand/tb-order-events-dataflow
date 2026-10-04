package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** One reconciled order; {@code classification} is one of the {@code MATCH}/mismatch constants. */
@DefaultCoder(SerializableCoder.class)
public record ReconciliationResult(
    String runDate,
    String orderId,
    String classification,
    Double legacyTotal,
    Double pubsubTotal,
    Double amountDiff,
    Long legacyLineCount,
    Long pubsubLineCount,
    String legacyStore,
    String pubsubStore,
    String detail)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String MATCH = "MATCH";
  public static final String MISSING_IN_PUBSUB = "MISSING_IN_PUBSUB";
  public static final String MISSING_IN_LEGACY = "MISSING_IN_LEGACY";
  public static final String AMOUNT_MISMATCH = "AMOUNT_MISMATCH";
  public static final String LINE_COUNT_MISMATCH = "LINE_COUNT_MISMATCH";
}
