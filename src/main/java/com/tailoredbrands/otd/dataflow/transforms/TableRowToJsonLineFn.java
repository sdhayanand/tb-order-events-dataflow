package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.util.Json;
import org.apache.beam.sdk.transforms.DoFn;

/** {@link TableRow} to one compact JSON line (used by the local/CI JSONL sinks). */
public class TableRowToJsonLineFn extends DoFn<TableRow, String> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element TableRow row, OutputReceiver<String> out) {
    out.output(Json.writeRow(row));
  }
}
