package com.tailoredbrands.otd.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Options of {@link OrderEventsStreamingPipeline}. Extends {@link DataflowPipelineOptions} so the
 * same interface carries {@code --project}, {@code --region}, {@code --streaming},
 * {@code --pubsubRootUrl} (emulator) and every other runner option. Each parameter is also
 * described in {@code metadata/order-events-streaming-metadata.json} for the Flex Template UI.
 */
public interface OrderEventsOptions extends DataflowPipelineOptions {

  @Description("Pub/Sub subscription for orders-v1, e.g. projects/p/subscriptions/orders-dataflow")
  @Validation.Required
  String getOrdersSubscription();

  void setOrdersSubscription(String value);

  @Description("Pub/Sub subscription for inventory-v1, e.g. projects/p/subscriptions/inventory-dataflow")
  @Validation.Required
  String getInventorySubscription();

  void setInventorySubscription(String value);

  @Description("Pub/Sub subscription for shipments-v1, e.g. projects/p/subscriptions/shipments-dataflow")
  @Validation.Required
  String getShipmentsSubscription();

  void setShipmentsSubscription(String value);

  @Description("BigQuery dataset that holds order_events, order_lines, ... (default otd)")
  @Default.String("otd")
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @Description("BigQuery project; defaults to --project")
  String getBigQueryProject();

  void setBigQueryProject(String value);

  @Description("Pub/Sub topic for dead letters, e.g. projects/p/topics/events-dlq (required unless --localOutputDir)")
  String getDeadLetterTopic();

  void setDeadLetterTopic(String value);

  @Description("Fixed window size in minutes for store_order_metrics (default 1)")
  @Default.Integer(1)
  Integer getMetricsWindowMinutes();

  void setMetricsWindowMinutes(Integer value);

  @Description("Allowed lateness in minutes for the metrics window (default 5)")
  @Default.Integer(5)
  Integer getMetricsAllowedLatenessMinutes();

  void setMetricsAllowedLatenessMinutes(Integer value);

  @Description("Optional CSV store_id,store_name,region,timezone (GCS or local) used to enrich orders")
  String getStoreReferenceGcsPath();

  void setStoreReferenceGcsPath(String value);

  @Description(
      "Local directory: when set (DirectRunner only) rows are written as JSON lines files per table "
          + "instead of BigQuery and dead letters are not published to Pub/Sub (used by CI e2e)")
  String getLocalOutputDir();

  void setLocalOutputDir(String value);

  @Description("Seconds between Storage Write API flushes in exactly-once mode (default 10)")
  @Default.Integer(10)
  Integer getBigQueryTriggeringSeconds();

  void setBigQueryTriggeringSeconds(Integer value);

  @Description("Use Storage Write API at-least-once mode (no shuffle, lower latency, possible duplicates)")
  @Default.Boolean(false)
  Boolean getUseStorageApiAtLeastOnce();

  void setUseStorageApiAtLeastOnce(Boolean value);
}
