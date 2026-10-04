package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.EventTypes;
import com.tailoredbrands.otd.dataflow.model.PubsubOrderSummary;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Reduces an {@code otd.order_events} row (from BigQuery or a local JSONL file) to the
 * {@link PubsubOrderSummary} keyed by order id. Only ORDER_CREATED rows are kept; the BigQuery
 * query already filters, the local file may not.
 */
public class OrderEventRowToSummaryFn extends DoFn<TableRow, KV<String, PubsubOrderSummary>> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(
      @Element TableRow row, OutputReceiver<KV<String, PubsubOrderSummary>> out) {
    String eventType = Rows.toStringOrNull(row.get("event_type"));
    if (eventType != null && !EventTypes.ORDER_CREATED.equals(eventType)) {
      return;
    }
    String orderId = Rows.toStringOrNull(row.get("order_id"));
    if (orderId == null || orderId.isBlank()) {
      return;
    }
    long lineCount;
    Object lc = row.get("line_count");
    if (lc != null) {
      lineCount = Rows.toLong(lc);
    } else {
      Object lines = row.get("lines");
      lineCount = lines instanceof List<?> l ? l.size() : 0L;
    }
    out.output(
        KV.of(
            orderId,
            new PubsubOrderSummary(
                orderId,
                Rows.toStringOrNull(row.get("store_id")),
                Rows.toStringOrNull(row.get("event_time")),
                Rows.toDouble(row.get("total_amount")),
                lineCount)));
  }
}
