package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.EnrichedOrderEvent;
import com.tailoredbrands.otd.dataflow.model.Order;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderLine;
import com.tailoredbrands.otd.dataflow.util.Times;
import org.apache.beam.sdk.transforms.DoFn;

/** Emits one {@code otd.order_lines} row per line of the order (denormalized for SKU analytics). */
public class OrderLinesToTableRowsFn extends DoFn<EnrichedOrderEvent, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element EnrichedOrderEvent e, OutputReceiver<TableRow> out) {
    OrderEvent ev = e.event();
    Order o = ev.order();
    String ingestedAt = Times.nowIso();
    for (OrderLine l : o.safeLines()) {
      TableRow row = new TableRow();
      Rows.put(row, "event_id", ev.eventId());
      Rows.put(row, "event_type", ev.eventType());
      Rows.put(row, "event_time", ev.eventTime());
      Rows.put(row, "order_id", o.orderId());
      Rows.put(row, "order_type", o.orderType());
      Rows.put(row, "channel", o.channel());
      Rows.put(row, "store_id", o.storeId());
      Rows.put(row, "region", e.region());
      Rows.put(row, "line_number", l.lineNumber());
      Rows.put(row, "sku", l.sku());
      Rows.put(row, "quantity", l.quantity());
      Rows.put(row, "unit_price", l.unitPrice());
      Rows.put(row, "line_amount", l.lineAmount());
      Rows.put(row, "fulfillment_type", l.fulfillmentType());
      if (l.alteration() != null) {
        Rows.put(row, "alteration_type", l.alteration().type());
        Rows.put(row, "alteration_measurement_inches", l.alteration().measurementInches());
        Rows.put(row, "tailor_shop_id", l.alteration().tailorShopId());
      }
      Rows.put(row, "ingested_at", ingestedAt);
      out.output(row);
    }
  }
}
