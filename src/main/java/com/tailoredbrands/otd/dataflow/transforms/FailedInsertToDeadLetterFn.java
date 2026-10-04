package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.util.Json;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Turns a row that BigQuery's Storage Write API rejected (schema mismatch, bad TIMESTAMP string,
 * oversized row...) into a {@link DeadLetter} so it still lands on {@code events-dlq}. The row is
 * serialized back to JSON as the dead-letter payload.
 */
public class FailedInsertToDeadLetterFn extends DoFn<BigQueryStorageApiInsertError, DeadLetter> {

  private static final long serialVersionUID = 1L;

  private final String tableName;
  private final Counter failedInserts;

  public FailedInsertToDeadLetterFn(String tableName) {
    this.tableName = tableName;
    this.failedInserts = Metrics.counter("otd.bigquery", tableName + "_failed_inserts");
  }

  @ProcessElement
  public void processElement(
      @Element BigQueryStorageApiInsertError error, OutputReceiver<DeadLetter> out) {
    failedInserts.inc();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("bigQueryTable", tableName);
    String payload = error.getRow() == null ? "" : Json.writeRow(error.getRow());
    out.output(
        DeadLetter.of(
            "bigquery:" + tableName,
            DeadLetter.STAGE_BIGQUERY_WRITE,
            String.valueOf(error.getErrorMessage()),
            payload,
            attributes,
            null,
            Times.nowIso()));
  }
}
