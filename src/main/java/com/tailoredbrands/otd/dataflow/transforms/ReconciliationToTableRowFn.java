package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.ReconciliationResult;
import com.tailoredbrands.otd.dataflow.util.Times;
import org.apache.beam.sdk.transforms.DoFn;

/** {@code otd.order_reconciliation} row, partitioned by {@code run_date}. */
public class ReconciliationToTableRowFn extends DoFn<ReconciliationResult, TableRow> {

  private static final long serialVersionUID = 1L;

  private final String runId;

  public ReconciliationToTableRowFn(String runId) {
    this.runId = runId;
  }

  @ProcessElement
  public void processElement(@Element ReconciliationResult r, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    Rows.put(row, "run_date", r.runDate());
    Rows.put(row, "run_id", runId);
    Rows.put(row, "order_id", r.orderId());
    Rows.put(row, "classification", r.classification());
    Rows.put(row, "legacy_total", r.legacyTotal());
    Rows.put(row, "pubsub_total", r.pubsubTotal());
    Rows.put(row, "amount_diff", r.amountDiff());
    Rows.put(row, "legacy_line_count", r.legacyLineCount());
    Rows.put(row, "pubsub_line_count", r.pubsubLineCount());
    Rows.put(row, "legacy_store", r.legacyStore());
    Rows.put(row, "pubsub_store", r.pubsubStore());
    Rows.put(row, "detail", r.detail());
    Rows.put(row, "checked_at", Times.nowIso());
    out.output(row);
  }
}
