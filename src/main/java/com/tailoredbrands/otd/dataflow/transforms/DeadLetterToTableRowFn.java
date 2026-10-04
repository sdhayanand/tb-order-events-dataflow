package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.util.Json;
import org.apache.beam.sdk.transforms.DoFn;

/** {@code otd.dead_letter} row: original payload kept verbatim as a STRING for replay. */
public class DeadLetterToTableRowFn extends DoFn<DeadLetter, TableRow> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element DeadLetter d, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    Rows.put(row, "original_topic", d.originalTopic());
    Rows.put(row, "stage", d.stage());
    Rows.put(row, "reason", d.reason());
    Rows.put(row, "message_id", d.messageId());
    Rows.put(row, "payload", d.payload());
    Rows.put(row, "attributes_json", Json.write(d.attributes()));
    Rows.put(row, "failed_at", d.failedAt());
    out.output(row);
  }
}
