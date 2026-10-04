package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.EnrichedOrderEvent;
import com.tailoredbrands.otd.dataflow.model.Order;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderLine;
import com.tailoredbrands.otd.dataflow.util.Json;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Flattens an enriched order event into one {@code otd.order_events} row: envelope + order header
 * as top-level columns and {@code lines} as a REPEATED RECORD (see docs/BIGQUERY-SCHEMAS.md).
 */
public class OrderEventToTableRowFn extends DoFn<EnrichedOrderEvent, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element EnrichedOrderEvent e, OutputReceiver<TableRow> out) {
    OrderEvent ev = e.event();
    Order o = ev.order();
    TableRow row = new TableRow();
    Rows.put(row, "event_id", ev.eventId());
    Rows.put(row, "event_type", ev.eventType());
    Rows.put(row, "event_time", ev.eventTime());
    Rows.put(row, "schema_version", ev.schemaVersion());
    Rows.put(row, "source", ev.source());
    Rows.put(row, "correlation_id", ev.correlationId());
    Rows.put(row, "legacy_message_id", ev.legacyMessageId());
    Rows.put(row, "message_id", e.messageId());
    Rows.put(row, "publish_time", e.publishTime());
    Rows.put(row, "order_id", o.orderId());
    Rows.put(row, "order_type", o.orderType());
    Rows.put(row, "channel", o.channel());
    Rows.put(row, "store_id", o.storeId());
    Rows.put(row, "store_name", e.storeName());
    Rows.put(row, "region", e.region());
    Rows.put(row, "customer_id", o.customerId());
    Rows.put(row, "ordered_at", o.orderedAt());
    Rows.put(row, "promised_date", o.promisedDate());
    Rows.put(row, "currency", o.currency());
    Rows.put(row, "total_amount", o.totalAmount());
    Rows.put(row, "line_count", (long) o.safeLines().size());
    Rows.put(row, "alteration_line_count", o.alterationLineCount());
    Rows.put(row, "is_rental", o.countsAsRental());
    Rows.put(row, "lines", lineRecords(o.safeLines()));
    Rows.put(row, "rental_json", o.rental() == null ? null : Json.write(o.rental()));
    Rows.put(row, "ship_to_json", o.shipTo() == null ? null : Json.write(o.shipTo()));
    Rows.put(row, "ingested_at", Times.nowIso());
    out.output(row);
  }

  static List<TableRow> lineRecords(List<OrderLine> lines) {
    List<TableRow> records = new ArrayList<>(lines.size());
    for (OrderLine l : lines) {
      TableRow r = new TableRow();
      Rows.put(r, "line_number", l.lineNumber());
      Rows.put(r, "sku", l.sku());
      Rows.put(r, "quantity", l.quantity());
      Rows.put(r, "unit_price", l.unitPrice());
      Rows.put(r, "fulfillment_type", l.fulfillmentType());
      if (l.alteration() != null) {
        Rows.put(r, "alteration_type", l.alteration().type());
        Rows.put(r, "alteration_measurement_inches", l.alteration().measurementInches());
        Rows.put(r, "tailor_shop_id", l.alteration().tailorShopId());
      }
      records.add(r);
    }
    return records;
  }
}
