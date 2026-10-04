package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.util.Json;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One JSON line to a {@link TableRow}; malformed lines are logged and skipped. */
public class JsonLineToTableRowFn extends DoFn<String, TableRow> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(JsonLineToTableRowFn.class);

  @ProcessElement
  public void processElement(@Element String line, OutputReceiver<TableRow> out) {
    if (line == null || line.isBlank()) {
      return;
    }
    try {
      Map<String, Object> map = Json.readMap(line);
      TableRow row = new TableRow();
      for (Map.Entry<String, Object> e : map.entrySet()) {
        row.set(e.getKey(), e.getValue());
      }
      out.output(row);
    } catch (Exception e) {
      LOG.warn("Skipping malformed JSON line: {}", e.getMessage());
    }
  }
}
