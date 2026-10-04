package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.model.ShipmentEvent;
import com.tailoredbrands.otd.dataflow.util.Times;
import org.apache.beam.sdk.transforms.DoFn;

/** One {@code otd.shipment_events} row per event. */
public class ShipmentEventToTableRowFn extends DoFn<Received<ShipmentEvent>, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element Received<ShipmentEvent> r, OutputReceiver<TableRow> out) {
    ShipmentEvent ev = r.payload();
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
    Rows.put(row, "tracking_number", ev.trackingNumber());
    Rows.put(row, "carrier", ev.carrier());
    Rows.put(row, "status", ev.status());
    Rows.put(row, "status_time", ev.statusTime());
    Rows.put(row, "location", ev.location());
    Rows.put(row, "ingested_at", Times.nowIso());
    out.output(row);
  }
}
