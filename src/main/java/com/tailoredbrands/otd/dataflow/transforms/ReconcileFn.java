package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import com.tailoredbrands.otd.dataflow.model.PubsubOrderSummary;
import com.tailoredbrands.otd.dataflow.model.ReconciliationResult;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Classifies one order id after the {@code CoGroupByKey} of legacy OMS orders and Pub/Sub-derived
 * {@code order_events}:
 *
 * <ul>
 *   <li>{@code MISSING_IN_PUBSUB}: in the legacy extract only (event never reached Pub/Sub, or
 *       was dead-lettered)
 *   <li>{@code MISSING_IN_LEGACY}: in BigQuery only (new API order that the legacy OMS never saw)
 *   <li>{@code AMOUNT_MISMATCH}: both present, {@code |legacy - pubsub| > tolerance}
 *   <li>{@code LINE_COUNT_MISMATCH}: both present, amounts agree, line counts differ
 *   <li>{@code MATCH}: everything agrees
 * </ul>
 *
 * Duplicates on either side (replays, repeated extracts) are collapsed: the Pub/Sub side keeps
 * the latest {@code event_time}, the legacy side keeps the first occurrence.
 */
public class ReconcileFn extends DoFn<KV<String, CoGbkResult>, ReconciliationResult> {

  private static final long serialVersionUID = 1L;

  private final TupleTag<LegacyOrder> legacyTag;
  private final TupleTag<PubsubOrderSummary> pubsubTag;
  private final String runDate;
  private final double amountTolerance;
  private final Map<String, Counter> counters = new HashMap<>();

  public ReconcileFn(
      TupleTag<LegacyOrder> legacyTag,
      TupleTag<PubsubOrderSummary> pubsubTag,
      String runDate,
      double amountTolerance) {
    this.legacyTag = legacyTag;
    this.pubsubTag = pubsubTag;
    this.runDate = runDate;
    this.amountTolerance = amountTolerance;
    for (String c :
        new String[] {
          ReconciliationResult.MATCH,
          ReconciliationResult.MISSING_IN_PUBSUB,
          ReconciliationResult.MISSING_IN_LEGACY,
          ReconciliationResult.AMOUNT_MISMATCH,
          ReconciliationResult.LINE_COUNT_MISMATCH
        }) {
      counters.put(c, Metrics.counter("otd.reconciliation", c));
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, CoGbkResult> element, OutputReceiver<ReconciliationResult> out) {
    String orderId = element.getKey();
    CoGbkResult result = element.getValue();

    LegacyOrder legacy = null;
    for (LegacyOrder l : result.getAll(legacyTag)) {
      legacy = l;
      break;
    }
    PubsubOrderSummary pubsub = null;
    for (PubsubOrderSummary p : result.getAll(pubsubTag)) {
      if (pubsub == null || isAfter(p.eventTime(), pubsub.eventTime())) {
        pubsub = p;
      }
    }

    ReconciliationResult r = classify(orderId, legacy, pubsub);
    counters.get(r.classification()).inc();
    out.output(r);
  }

  ReconciliationResult classify(String orderId, LegacyOrder legacy, PubsubOrderSummary pubsub) {
    if (legacy == null && pubsub == null) {
      throw new IllegalStateException("CoGroupByKey produced a key with no values: " + orderId);
    }
    if (pubsub == null) {
      return new ReconciliationResult(
          runDate,
          orderId,
          ReconciliationResult.MISSING_IN_PUBSUB,
          legacy.effectiveTotal(),
          null,
          null,
          (long) legacy.safeLines().size(),
          null,
          legacy.storeNbr(),
          null,
          "order present in legacy OMS extract " + legacy.sourceFile() + " but no ORDER_CREATED event in BigQuery");
    }
    if (legacy == null) {
      return new ReconciliationResult(
          runDate,
          orderId,
          ReconciliationResult.MISSING_IN_LEGACY,
          null,
          pubsub.totalAmount(),
          null,
          null,
          pubsub.lineCount(),
          null,
          pubsub.storeId(),
          "ORDER_CREATED event in BigQuery but order absent from legacy OMS extract");
    }
    double legacyTotal = legacy.effectiveTotal();
    double diff = legacyTotal - pubsub.totalAmount();
    long legacyLines = legacy.safeLines().size();
    String classification;
    String detail;
    if (Math.abs(diff) > amountTolerance) {
      classification = ReconciliationResult.AMOUNT_MISMATCH;
      detail = String.format("legacy total %.2f vs pubsub total %.2f (tolerance %.2f)", legacyTotal, pubsub.totalAmount(), amountTolerance);
    } else if (legacyLines != pubsub.lineCount()) {
      classification = ReconciliationResult.LINE_COUNT_MISMATCH;
      detail = "legacy lines " + legacyLines + " vs pubsub lines " + pubsub.lineCount();
    } else {
      classification = ReconciliationResult.MATCH;
      detail = null;
    }
    return new ReconciliationResult(
        runDate,
        orderId,
        classification,
        legacyTotal,
        pubsub.totalAmount(),
        diff,
        legacyLines,
        pubsub.lineCount(),
        legacy.storeNbr(),
        pubsub.storeId(),
        detail);
  }

  private static boolean isAfter(String a, String b) {
    if (a == null) {
      return false;
    }
    if (b == null) {
      return true;
    }
    return a.compareTo(b) > 0;
  }
}
