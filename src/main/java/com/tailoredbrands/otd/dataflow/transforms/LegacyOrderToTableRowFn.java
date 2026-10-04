package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.LegacyLine;
import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;

/** {@code otd.legacy_oms_orders} row, partitioned by {@code extract_date} (= the run date). */
public class LegacyOrderToTableRowFn extends DoFn<LegacyOrder, TableRow> {

  private static final long serialVersionUID = 1L;

  private final String extractDate;

  public LegacyOrderToTableRowFn(String extractDate) {
    this.extractDate = extractDate;
  }

  @ProcessElement
  public void processElement(@Element LegacyOrder o, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    Rows.put(row, "extract_date", extractDate);
    Rows.put(row, "extract_file", o.sourceFile());
    Rows.put(row, "order_nbr", o.orderNbr());
    Rows.put(row, "order_type", o.orderType());
    Rows.put(row, "store_nbr", o.storeNbr());
    Rows.put(row, "cust_nbr", o.custNbr());
    Rows.put(row, "order_date", o.orderDate());
    Rows.put(row, "total_amount", o.effectiveTotal());
    Rows.put(row, "line_count", (long) o.safeLines().size());
    List<TableRow> lines = new ArrayList<>();
    for (LegacyLine l : o.safeLines()) {
      TableRow lr = new TableRow();
      Rows.put(lr, "line_nbr", l.lineNbr());
      Rows.put(lr, "sku", l.sku());
      Rows.put(lr, "qty", l.qty());
      Rows.put(lr, "price", l.price());
      Rows.put(lr, "fulfill_type", l.fulfillType());
      lines.add(lr);
    }
    row.set("lines", lines);
    Rows.put(row, "loaded_at", Times.nowIso());
    out.output(row);
  }
}
