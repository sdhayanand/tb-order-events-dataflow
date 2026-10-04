package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.StoreMetrics;
import com.tailoredbrands.otd.dataflow.util.Times;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;

/**
 * Formats one {@code store_order_metrics} row per (store, window, pane). Because the window uses
 * early/on-time/late firings with accumulating panes, the same (store, window) can be emitted
 * several times with increasing {@code pane_index}; consumers take the row with the highest
 * {@code pane_index} (or {@code pane_timing = 'ON_TIME'}) per key. {@code is_last} is only true
 * when the window was garbage collected after the allowed lateness.
 */
public class StoreMetricsToTableRowFn extends DoFn<KV<String, StoreMetrics>, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    KV<String, StoreMetrics> kv = c.element();
    StoreMetrics m = kv.getValue();
    PaneInfo pane = c.pane();

    String windowStart;
    String windowEnd;
    if (window instanceof IntervalWindow iw) {
      windowStart = Times.toIso(iw.start());
      windowEnd = Times.toIso(iw.end());
    } else {
      // GlobalWindow (only in tests that skip windowing): use the element timestamp for both.
      windowStart = Times.toIso(c.timestamp());
      windowEnd = windowStart;
    }

    TableRow row = new TableRow();
    row.set("store_id", kv.getKey());
    row.set("window_start", windowStart);
    row.set("window_end", windowEnd);
    row.set("pane_timing", pane.getTiming().name());
    row.set("pane_index", pane.getIndex());
    row.set("is_last", pane.isLast());
    row.set("orders", m.orders());
    row.set("revenue", m.revenue());
    row.set("alteration_lines", m.alterationLines());
    row.set("rental_orders", m.rentalOrders());
    row.set("emitted_at", Times.nowIso());
    c.output(row);
  }
}
