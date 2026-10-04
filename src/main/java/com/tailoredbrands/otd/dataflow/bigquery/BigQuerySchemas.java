package com.tailoredbrands.otd.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;

/**
 * Explicit {@link TableSchema}s for every table the pipelines create ({@code CREATE_IF_NEEDED}).
 * Terraform ({@code tb-platform-infra/modules/bigquery}) owns the dataset and may pre-create the
 * tables with the same schemas; these definitions must stay additive-only (ARCHITECTURE.md
 * section 8, schema evolution). Human-readable copy: docs/BIGQUERY-SCHEMAS.md.
 *
 * <p>Legacy type names ({@code INTEGER}, {@code FLOAT}, {@code RECORD}, {@code BOOLEAN}) are used
 * because they are accepted by every BigQuery API surface Beam talks to (load jobs, streaming
 * inserts and the Storage Write API proto conversion).
 */
public final class BigQuerySchemas {

  public static final String ORDER_EVENTS = "order_events";
  public static final String ORDER_LINES = "order_lines";
  public static final String INVENTORY_EVENTS = "inventory_events";
  public static final String SHIPMENT_EVENTS = "shipment_events";
  public static final String STORE_ORDER_METRICS = "store_order_metrics";
  public static final String DEAD_LETTER = "dead_letter";
  public static final String LEGACY_OMS_ORDERS = "legacy_oms_orders";
  public static final String ORDER_RECONCILIATION = "order_reconciliation";

  private BigQuerySchemas() {}

  // ---------------------------------------------------------------- streaming tables

  public static TableSchema orderEvents() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("event_id", "STRING"));
    f.add(required("event_type", "STRING"));
    f.add(required("event_time", "TIMESTAMP"));
    f.add(nullable("schema_version", "STRING"));
    f.add(nullable("source", "STRING"));
    f.add(nullable("correlation_id", "STRING"));
    f.add(nullable("legacy_message_id", "STRING"));
    f.add(nullable("message_id", "STRING"));
    f.add(nullable("publish_time", "TIMESTAMP"));
    f.add(required("order_id", "STRING"));
    f.add(nullable("order_type", "STRING"));
    f.add(nullable("channel", "STRING"));
    f.add(required("store_id", "STRING"));
    f.add(nullable("store_name", "STRING"));
    f.add(nullable("region", "STRING"));
    f.add(nullable("customer_id", "STRING"));
    f.add(nullable("ordered_at", "TIMESTAMP"));
    f.add(nullable("promised_date", "DATE"));
    f.add(nullable("currency", "STRING"));
    f.add(nullable("total_amount", "FLOAT"));
    f.add(nullable("line_count", "INTEGER"));
    f.add(nullable("alteration_line_count", "INTEGER"));
    f.add(nullable("is_rental", "BOOLEAN"));
    f.add(repeatedRecord("lines", orderLineRecordFields()));
    f.add(nullable("rental_json", "STRING"));
    f.add(nullable("ship_to_json", "STRING"));
    f.add(nullable("ingested_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  private static List<TableFieldSchema> orderLineRecordFields() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(nullable("line_number", "INTEGER"));
    f.add(nullable("sku", "STRING"));
    f.add(nullable("quantity", "INTEGER"));
    f.add(nullable("unit_price", "FLOAT"));
    f.add(nullable("fulfillment_type", "STRING"));
    f.add(nullable("alteration_type", "STRING"));
    f.add(nullable("alteration_measurement_inches", "FLOAT"));
    f.add(nullable("tailor_shop_id", "STRING"));
    return f;
  }

  public static TableSchema orderLines() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("event_id", "STRING"));
    f.add(required("event_type", "STRING"));
    f.add(required("event_time", "TIMESTAMP"));
    f.add(required("order_id", "STRING"));
    f.add(nullable("order_type", "STRING"));
    f.add(nullable("channel", "STRING"));
    f.add(required("store_id", "STRING"));
    f.add(nullable("region", "STRING"));
    f.add(required("line_number", "INTEGER"));
    f.add(required("sku", "STRING"));
    f.add(nullable("quantity", "INTEGER"));
    f.add(nullable("unit_price", "FLOAT"));
    f.add(nullable("line_amount", "FLOAT"));
    f.add(nullable("fulfillment_type", "STRING"));
    f.add(nullable("alteration_type", "STRING"));
    f.add(nullable("alteration_measurement_inches", "FLOAT"));
    f.add(nullable("tailor_shop_id", "STRING"));
    f.add(nullable("ingested_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  public static TableSchema inventoryEvents() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("event_id", "STRING"));
    f.add(required("event_type", "STRING"));
    f.add(required("event_time", "TIMESTAMP"));
    f.add(nullable("schema_version", "STRING"));
    f.add(nullable("source", "STRING"));
    f.add(nullable("correlation_id", "STRING"));
    f.add(nullable("message_id", "STRING"));
    f.add(nullable("publish_time", "TIMESTAMP"));
    f.add(required("order_id", "STRING"));
    f.add(required("store_id", "STRING"));
    f.add(nullable("line_count", "INTEGER"));
    List<TableFieldSchema> line = new ArrayList<>();
    line.add(nullable("line_number", "INTEGER"));
    line.add(nullable("sku", "STRING"));
    line.add(nullable("quantity", "INTEGER"));
    line.add(nullable("status", "STRING"));
    line.add(nullable("location_id", "STRING"));
    f.add(repeatedRecord("lines", line));
    f.add(nullable("ingested_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  public static TableSchema shipmentEvents() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("event_id", "STRING"));
    f.add(required("event_type", "STRING"));
    f.add(required("event_time", "TIMESTAMP"));
    f.add(nullable("schema_version", "STRING"));
    f.add(nullable("source", "STRING"));
    f.add(nullable("correlation_id", "STRING"));
    f.add(nullable("message_id", "STRING"));
    f.add(nullable("publish_time", "TIMESTAMP"));
    f.add(required("order_id", "STRING"));
    f.add(nullable("tracking_number", "STRING"));
    f.add(nullable("carrier", "STRING"));
    f.add(required("status", "STRING"));
    f.add(nullable("status_time", "TIMESTAMP"));
    f.add(nullable("location", "STRING"));
    f.add(nullable("ingested_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  public static TableSchema storeOrderMetrics() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("store_id", "STRING"));
    f.add(required("window_start", "TIMESTAMP"));
    f.add(required("window_end", "TIMESTAMP"));
    f.add(required("pane_timing", "STRING"));
    f.add(nullable("pane_index", "INTEGER"));
    f.add(nullable("is_last", "BOOLEAN"));
    f.add(required("orders", "INTEGER"));
    f.add(required("revenue", "FLOAT"));
    f.add(required("alteration_lines", "INTEGER"));
    f.add(required("rental_orders", "INTEGER"));
    f.add(nullable("emitted_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  public static TableSchema deadLetter() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("original_topic", "STRING"));
    f.add(required("stage", "STRING"));
    f.add(required("reason", "STRING"));
    f.add(nullable("message_id", "STRING"));
    f.add(nullable("payload", "STRING"));
    f.add(nullable("attributes_json", "STRING"));
    f.add(required("failed_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  // ---------------------------------------------------------------- batch tables

  public static TableSchema legacyOmsOrders() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("extract_date", "DATE"));
    f.add(nullable("extract_file", "STRING"));
    f.add(required("order_nbr", "STRING"));
    f.add(nullable("order_type", "STRING"));
    f.add(nullable("store_nbr", "STRING"));
    f.add(nullable("cust_nbr", "STRING"));
    f.add(nullable("order_date", "STRING"));
    f.add(nullable("total_amount", "FLOAT"));
    f.add(nullable("line_count", "INTEGER"));
    List<TableFieldSchema> line = new ArrayList<>();
    line.add(nullable("line_nbr", "INTEGER"));
    line.add(nullable("sku", "STRING"));
    line.add(nullable("qty", "INTEGER"));
    line.add(nullable("price", "FLOAT"));
    line.add(nullable("fulfill_type", "STRING"));
    f.add(repeatedRecord("lines", line));
    f.add(nullable("loaded_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  public static TableSchema orderReconciliation() {
    List<TableFieldSchema> f = new ArrayList<>();
    f.add(required("run_date", "DATE"));
    f.add(nullable("run_id", "STRING"));
    f.add(required("order_id", "STRING"));
    f.add(required("classification", "STRING"));
    f.add(nullable("legacy_total", "FLOAT"));
    f.add(nullable("pubsub_total", "FLOAT"));
    f.add(nullable("amount_diff", "FLOAT"));
    f.add(nullable("legacy_line_count", "INTEGER"));
    f.add(nullable("pubsub_line_count", "INTEGER"));
    f.add(nullable("legacy_store", "STRING"));
    f.add(nullable("pubsub_store", "STRING"));
    f.add(nullable("detail", "STRING"));
    f.add(nullable("checked_at", "TIMESTAMP"));
    return new TableSchema().setFields(f);
  }

  // ---------------------------------------------------------------- helpers

  static TableFieldSchema required(String name, String type) {
    return new TableFieldSchema().setName(name).setType(type).setMode("REQUIRED");
  }

  static TableFieldSchema nullable(String name, String type) {
    return new TableFieldSchema().setName(name).setType(type).setMode("NULLABLE");
  }

  static TableFieldSchema repeatedRecord(String name, List<TableFieldSchema> fields) {
    return new TableFieldSchema().setName(name).setType("RECORD").setMode("REPEATED").setFields(fields);
  }
}
