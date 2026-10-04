package com.tailoredbrands.otd.dataflow.bigquery;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.joda.time.Duration;

/**
 * Factory for the {@link BigQueryIO.Write} transforms used by both pipelines so that every table is
 * written the same way: explicit schema, {@code CREATE_IF_NEEDED}, {@code WRITE_APPEND}, day
 * partitioning and clustering, and the Storage Write API.
 *
 * <h3>Exactly-once vs at-least-once</h3>
 *
 * <ul>
 *   <li>{@link Method#STORAGE_WRITE_API} (default here) gives exactly-once semantics in streaming:
 *       rows are buffered per key/window, flushed every {@code triggeringFrequency} into
 *       committed-type streams, and offsets are tracked so retries never duplicate. It needs a
 *       triggering frequency and a fixed number of streams (or auto-sharding) and introduces a
 *       GroupByKey, which costs latency and shuffle.
 *   <li>{@link Method#STORAGE_API_AT_LEAST_ONCE} skips the shuffle and writes through the default
 *       stream as rows arrive: lower latency and cost, but a worker retry can append a row twice.
 *       Fine for append-only event tables where downstream queries dedupe on {@code event_id}.
 * </ul>
 */
public final class BigQueryWrites {

  private BigQueryWrites() {}

  public static TableReference table(String project, String dataset, String table) {
    return new TableReference().setProjectId(project).setDatasetId(dataset).setTableId(table);
  }

  /**
   * Streaming write.
   *
   * @param partitionField TIMESTAMP/DATE column for day partitioning, or {@code null} for
   *     ingestion-time partitioning
   * @param clusterFields clustering columns, may be empty
   * @param atLeastOnce use {@code STORAGE_API_AT_LEAST_ONCE} instead of exactly-once
   */
  public static BigQueryIO.Write<TableRow> streaming(
      TableReference table,
      TableSchema schema,
      String partitionField,
      List<String> clusterFields,
      boolean atLeastOnce,
      Duration triggeringFrequency) {
    BigQueryIO.Write<TableRow> write = base(table, schema, partitionField, clusterFields);
    if (atLeastOnce) {
      return write.withMethod(Method.STORAGE_API_AT_LEAST_ONCE);
    }
    return write
        .withMethod(Method.STORAGE_WRITE_API)
        .withTriggeringFrequency(triggeringFrequency)
        .withNumStorageWriteApiStreams(1);
  }

  /** Batch write (no triggering frequency needed; Beam commits streams at the end of the bundle set). */
  public static BigQueryIO.Write<TableRow> batch(
      TableReference table, TableSchema schema, String partitionField, List<String> clusterFields) {
    return base(table, schema, partitionField, clusterFields).withMethod(Method.STORAGE_WRITE_API);
  }

  private static BigQueryIO.Write<TableRow> base(
      TableReference table, TableSchema schema, String partitionField, List<String> clusterFields) {
    TimePartitioning partitioning = new TimePartitioning().setType("DAY");
    if (partitionField != null) {
      partitioning.setField(partitionField);
    }
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(table)
            .withSchema(schema)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withTimePartitioning(partitioning);
    if (clusterFields != null && !clusterFields.isEmpty()) {
      write = write.withClustering(new Clustering().setFields(clusterFields));
    }
    return write;
  }
}
