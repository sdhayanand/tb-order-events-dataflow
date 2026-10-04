package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.InventoryEvent;
import com.tailoredbrands.otd.dataflow.model.InventoryLine;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;

/** One {@code otd.inventory_events} row per event, lines as a REPEATED RECORD. */
public class InventoryEventToTableRowFn extends DoFn<Received<InventoryEvent>, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element Received<InventoryEvent> r, OutputReceiver<TableRow> out) {
    InventoryEvent ev = r.payload();
    TableRow row = new TableRow();
    Rows.put(row, "event_id", ev.eventId());
    Rows.put(row, "event_type", ev.eventType());
    Rows.put(row, "event_time", ev.eventTime());
    Rows.put(row, "schema_version", ev.schemaVersion());
    Rows.put(row, "source", ev.source());
    Rows.put(row, "correlation_id", ev.correlationId());
    Rows.put(row, "message_id", r.messageId());
    Rows.put(row, "publish_time", r.publishTime());
    Rows.put(row, "order_id", ev.orderId());
    Rows.put(row, "store_id", ev.storeId());
    Rows.put(row, "line_count", (long) ev.safeLines().size());
    List<TableRow> lines = new ArrayList<>();
    for (InventoryLine l : ev.safeLines()) {
      TableRow lr = new TableRow();
      Rows.put(lr, "line_number", l.lineNumber());
      Rows.put(lr, "sku", l.sku());
      Rows.put(lr, "quantity", l.quantity());
      Rows.put(lr, "status", l.status());
      Rows.put(lr, "location_id", l.locationId());
      lines.add(lr);
    }
    row.set("lines", lines);
    Rows.put(row, "ingested_at", Times.nowIso());
    out.output(row);
  }
}
