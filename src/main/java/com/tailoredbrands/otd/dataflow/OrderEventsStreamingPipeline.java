package com.tailoredbrands.otd.dataflow;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.tailoredbrands.otd.dataflow.bigquery.BigQuerySchemas;
import com.tailoredbrands.otd.dataflow.bigquery.BigQueryWrites;
import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.model.EnrichedOrderEvent;
import com.tailoredbrands.otd.dataflow.model.InventoryEvent;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.model.ShipmentEvent;
import com.tailoredbrands.otd.dataflow.model.StoreMetrics;
import com.tailoredbrands.otd.dataflow.model.StoreRef;
import com.tailoredbrands.otd.dataflow.transforms.DeadLetterToPubsubMessageFn;
import com.tailoredbrands.otd.dataflow.transforms.DeadLetterToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.EnrichOrderFn;
import com.tailoredbrands.otd.dataflow.transforms.FailedInsertToDeadLetterFn;
import com.tailoredbrands.otd.dataflow.transforms.InventoryEventToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.InventoryEventValidator;
import com.tailoredbrands.otd.dataflow.transforms.LocalJsonLinesWriteFn;
import com.tailoredbrands.otd.dataflow.transforms.OrderEventToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.OrderEventValidator;
import com.tailoredbrands.otd.dataflow.transforms.OrderLinesToTableRowsFn;
import com.tailoredbrands.otd.dataflow.transforms.ParseAndValidateFn;
import com.tailoredbrands.otd.dataflow.transforms.ShipmentEventToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.ShipmentEventValidator;
import com.tailoredbrands.otd.dataflow.transforms.StoreMetricsCombineFn;
import com.tailoredbrands.otd.dataflow.transforms.StoreMetricsToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.StoreReferenceView;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming pipeline: Pub/Sub ({@code orders-v1}, {@code inventory-v1}, {@code shipments-v1}) to
 * BigQuery {@code otd.*} with validation, store enrichment, windowed store metrics and a dead-letter
 * path to {@code events-dlq} + {@code otd.dead_letter}.
 *
 * <pre>
 *  orders-dataflow ──▶ ParseOrders ──┬▶ EnrichOrders ──┬▶ order_events (BQ)
 *                                    │                 └▶ order_lines  (BQ)
 *                                    ├▶ ORDER_CREATED ▶ 1-min windows ▶ Combine.perKey ▶ store_order_metrics (BQ)
 *                                    └▶ DeadLetter ─┐
 *  inventory-dataflow ▶ ParseInventory ─┬▶ inventory_events (BQ)
 *                                       └▶ DeadLetter ─┤
 *  shipments-dataflow ▶ ParseShipments ─┬▶ shipment_events (BQ)
 *                                       └▶ DeadLetter ─┤
 *  BigQuery failed inserts ────────────────────────────┤
 *                                                      ▼
 *                                   Flatten ▶ events-dlq (Pub/Sub) + dead_letter (BQ)
 * </pre>
 *
 * The {@code build} method is runner-agnostic and reused by the tests ({@code TestStream} instead
 * of {@code PubsubIO}) through the package-private {@code parse*} / {@code computeStoreMetrics}
 * helpers.
 */
public final class OrderEventsStreamingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(OrderEventsStreamingPipeline.class);

  static final String ORDERS_TOPIC = "orders-v1";
  static final String INVENTORY_TOPIC = "inventory-v1";
  static final String SHIPMENTS_TOPIC = "shipments-v1";

  static final TupleTag<Received<OrderEvent>> ORDERS_TAG = new TupleTag<Received<OrderEvent>>() {};
  static final TupleTag<Received<InventoryEvent>> INVENTORY_TAG =
      new TupleTag<Received<InventoryEvent>>() {};
  static final TupleTag<Received<ShipmentEvent>> SHIPMENTS_TAG =
      new TupleTag<Received<ShipmentEvent>>() {};
  static final TupleTag<DeadLetter> DEAD_LETTER_TAG = new TupleTag<DeadLetter>() {};

  static final SerializableCoder<Received<OrderEvent>> RECEIVED_ORDER_CODER =
      SerializableCoder.of(new TypeDescriptor<Received<OrderEvent>>() {});
  static final SerializableCoder<Received<InventoryEvent>> RECEIVED_INVENTORY_CODER =
      SerializableCoder.of(new TypeDescriptor<Received<InventoryEvent>>() {});
  static final SerializableCoder<Received<ShipmentEvent>> RECEIVED_SHIPMENT_CODER =
      SerializableCoder.of(new TypeDescriptor<Received<ShipmentEvent>>() {});

  private OrderEventsStreamingPipeline() {}

  public static void main(String[] args) {
    PipelineOptionsFactory.register(OrderEventsOptions.class);
    OrderEventsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(OrderEventsOptions.class);
    options.setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);
    build(pipeline, options);
    pipeline.run().waitUntilFinish();
  }

  /** Assembles the full DAG on {@code pipeline}. */
  public static void build(Pipeline pipeline, OrderEventsOptions options) {
    boolean local = isLocalMode(options);
    if (local && options.getRunner() != null
        && "DataflowRunner".equals(options.getRunner().getSimpleName())) {
      throw new IllegalArgumentException("--localOutputDir is only supported with the DirectRunner");
    }
    if (!local && (options.getDeadLetterTopic() == null || options.getDeadLetterTopic().isBlank())) {
      throw new IllegalArgumentException("--deadLetterTopic is required unless --localOutputDir is set");
    }

    // 1. Sources: Pub/Sub subscriptions, keep attributes + message id (dedup / lineage)
    PCollection<PubsubMessage> ordersRaw =
        pipeline.apply(
            "ReadOrders",
            PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromSubscription(options.getOrdersSubscription()));
    PCollection<PubsubMessage> inventoryRaw =
        pipeline.apply(
            "ReadInventory",
            PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromSubscription(options.getInventorySubscription()));
    PCollection<PubsubMessage> shipmentsRaw =
        pipeline.apply(
            "ReadShipments",
            PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromSubscription(options.getShipmentsSubscription()));

    // 2. Parse + validate, invalid messages to the dead-letter tag
    PCollectionTuple ordersParsed = parseOrders(ordersRaw);
    PCollectionTuple inventoryParsed = parseInventory(inventoryRaw);
    PCollectionTuple shipmentsParsed = parseShipments(shipmentsRaw);

    PCollection<Received<OrderEvent>> orders = ordersParsed.get(ORDERS_TAG);
    PCollection<Received<InventoryEvent>> inventory = inventoryParsed.get(INVENTORY_TAG);
    PCollection<Received<ShipmentEvent>> shipments = shipmentsParsed.get(SHIPMENTS_TAG);

    // 3. Enrich orders with the store reference side input
    PCollectionView<Map<String, StoreRef>> storeView =
        StoreReferenceView.build(pipeline, options.getStoreReferenceGcsPath());
    PCollection<EnrichedOrderEvent> enriched =
        orders
            .apply(
                "EnrichOrders", ParDo.of(new EnrichOrderFn(storeView)).withSideInputs(storeView))
            .setCoder(SerializableCoder.of(EnrichedOrderEvent.class));

    // 4. Rows for the event tables
    PCollection<TableRow> orderEventRows =
        enriched
            .apply("OrderEventsToRows", ParDo.of(new OrderEventToTableRowFn()))
            .setCoder(TableRowJsonCoder.of());
    PCollection<TableRow> orderLineRows =
        enriched
            .apply("OrderLinesToRows", ParDo.of(new OrderLinesToTableRowsFn()))
            .setCoder(TableRowJsonCoder.of());
    PCollection<TableRow> inventoryRows =
        inventory
            .apply("InventoryEventsToRows", ParDo.of(new InventoryEventToTableRowFn()))
            .setCoder(TableRowJsonCoder.of());
    PCollection<TableRow> shipmentRows =
        shipments
            .apply("ShipmentEventsToRows", ParDo.of(new ShipmentEventToTableRowFn()))
            .setCoder(TableRowJsonCoder.of());

    // 5. Windowed per-store metrics
    PCollection<TableRow> metricsRows =
        computeStoreMetrics(
            orders,
            Duration.standardMinutes(options.getMetricsWindowMinutes()),
            Duration.standardMinutes(options.getMetricsAllowedLatenessMinutes()));

    // 6. Dead letters from the three parsers
    PCollectionList<DeadLetter> deadLetterSources =
        PCollectionList.of(ordersParsed.get(DEAD_LETTER_TAG))
            .and(inventoryParsed.get(DEAD_LETTER_TAG))
            .and(shipmentsParsed.get(DEAD_LETTER_TAG));

    if (local) {
      LOG.info("Local mode: writing JSON lines to {} and skipping Pub/Sub DLQ", options.getLocalOutputDir());
      String dir = options.getLocalOutputDir();
      writeLocal(orderEventRows, dir, BigQuerySchemas.ORDER_EVENTS);
      writeLocal(orderLineRows, dir, BigQuerySchemas.ORDER_LINES);
      writeLocal(inventoryRows, dir, BigQuerySchemas.INVENTORY_EVENTS);
      writeLocal(shipmentRows, dir, BigQuerySchemas.SHIPMENT_EVENTS);
      writeLocal(metricsRows, dir, BigQuerySchemas.STORE_ORDER_METRICS);
      PCollection<TableRow> deadLetterRows =
          deadLetterSources
              .apply("FlattenDeadLetters", Flatten.pCollections())
              .apply("DeadLettersToRows", ParDo.of(new DeadLetterToTableRowFn()))
              .setCoder(TableRowJsonCoder.of());
      writeLocal(deadLetterRows, dir, BigQuerySchemas.DEAD_LETTER);
      return;
    }

    // 7. BigQuery sinks (Storage Write API); rejected rows join the dead-letter stream
    String project =
        options.getBigQueryProject() == null || options.getBigQueryProject().isBlank()
            ? options.getProject()
            : options.getBigQueryProject();
    String dataset = options.getBigQueryDataset();
    boolean atLeastOnce = Boolean.TRUE.equals(options.getUseStorageApiAtLeastOnce());
    Duration frequency = Duration.standardSeconds(options.getBigQueryTriggeringSeconds());

    deadLetterSources =
        deadLetterSources
            .and(
                writeBigQuery(
                    orderEventRows,
                    BigQueryWrites.table(project, dataset, BigQuerySchemas.ORDER_EVENTS),
                    BigQuerySchemas.orderEvents(),
                    "event_time",
                    List.of("store_id"),
                    atLeastOnce,
                    frequency))
            .and(
                writeBigQuery(
                    orderLineRows,
                    BigQueryWrites.table(project, dataset, BigQuerySchemas.ORDER_LINES),
                    BigQuerySchemas.orderLines(),
                    "event_time",
                    List.of("sku"),
                    atLeastOnce,
                    frequency))
            .and(
                writeBigQuery(
                    inventoryRows,
                    BigQueryWrites.table(project, dataset, BigQuerySchemas.INVENTORY_EVENTS),
                    BigQuerySchemas.inventoryEvents(),
                    "event_time",
                    List.of("store_id"),
                    atLeastOnce,
                    frequency))
            .and(
                writeBigQuery(
                    shipmentRows,
                    BigQueryWrites.table(project, dataset, BigQuerySchemas.SHIPMENT_EVENTS),
                    BigQuerySchemas.shipmentEvents(),
                    "event_time",
                    List.of("order_id"),
                    atLeastOnce,
                    frequency))
            .and(
                writeBigQuery(
                    metricsRows,
                    BigQueryWrites.table(project, dataset, BigQuerySchemas.STORE_ORDER_METRICS),
                    BigQuerySchemas.storeOrderMetrics(),
                    "window_start",
                    List.of("store_id"),
                    atLeastOnce,
                    frequency));

    PCollection<DeadLetter> deadLetters =
        deadLetterSources.apply("FlattenDeadLetters", Flatten.pCollections());

    // 8. DLQ: Pub/Sub topic (for alerting + replay tooling) and BigQuery (for analysis)
    deadLetters
        .apply("DeadLettersToPubsubMessages", ParDo.of(new DeadLetterToPubsubMessageFn()))
        .setCoder(PubsubMessageWithAttributesCoder.of())
        .apply("WriteDeadLetterTopic", PubsubIO.writeMessages().to(options.getDeadLetterTopic()));

    deadLetters
        .apply("DeadLettersToRows", ParDo.of(new DeadLetterToTableRowFn()))
        .setCoder(TableRowJsonCoder.of())
        .apply(
            "WriteDeadLetterTable",
            BigQueryWrites.streaming(
                BigQueryWrites.table(project, dataset, BigQuerySchemas.DEAD_LETTER),
                BigQuerySchemas.deadLetter(),
                null,
                List.of(),
                atLeastOnce,
                frequency));
  }

  // ------------------------------------------------------------------ reusable pieces

  static boolean isLocalMode(OrderEventsOptions options) {
    return options.getLocalOutputDir() != null && !options.getLocalOutputDir().isBlank();
  }

  /** orders-v1 messages to {@link #ORDERS_TAG} / {@link #DEAD_LETTER_TAG}. */
  static PCollectionTuple parseOrders(PCollection<PubsubMessage> messages) {
    PCollectionTuple tuple =
        messages.apply(
            "ParseOrders",
            ParDo.of(
                    new ParseAndValidateFn<>(
                        OrderEvent.class,
                        new OrderEventValidator(),
                        "orders",
                        ORDERS_TOPIC,
                        DEAD_LETTER_TAG))
                .withOutputTags(ORDERS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));
    tuple.get(ORDERS_TAG).setCoder(RECEIVED_ORDER_CODER);
    tuple.get(DEAD_LETTER_TAG).setCoder(SerializableCoder.of(DeadLetter.class));
    return tuple;
  }

  /** inventory-v1 messages to {@link #INVENTORY_TAG} / {@link #DEAD_LETTER_TAG}. */
  static PCollectionTuple parseInventory(PCollection<PubsubMessage> messages) {
    PCollectionTuple tuple =
        messages.apply(
            "ParseInventory",
            ParDo.of(
                    new ParseAndValidateFn<>(
                        InventoryEvent.class,
                        new InventoryEventValidator(),
                        "inventory",
                        INVENTORY_TOPIC,
                        DEAD_LETTER_TAG))
                .withOutputTags(INVENTORY_TAG, TupleTagList.of(DEAD_LETTER_TAG)));
    tuple.get(INVENTORY_TAG).setCoder(RECEIVED_INVENTORY_CODER);
    tuple.get(DEAD_LETTER_TAG).setCoder(SerializableCoder.of(DeadLetter.class));
    return tuple;
  }

  /** shipments-v1 messages to {@link #SHIPMENTS_TAG} / {@link #DEAD_LETTER_TAG}. */
  static PCollectionTuple parseShipments(PCollection<PubsubMessage> messages) {
    PCollectionTuple tuple =
        messages.apply(
            "ParseShipments",
            ParDo.of(
                    new ParseAndValidateFn<>(
                        ShipmentEvent.class,
                        new ShipmentEventValidator(),
                        "shipments",
                        SHIPMENTS_TOPIC,
                        DEAD_LETTER_TAG))
                .withOutputTags(SHIPMENTS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));
    tuple.get(SHIPMENTS_TAG).setCoder(RECEIVED_SHIPMENT_CODER);
    tuple.get(DEAD_LETTER_TAG).setCoder(SerializableCoder.of(DeadLetter.class));
    return tuple;
  }

  /**
   * ORDER_CREATED events keyed by store, in fixed event-time windows with:
   *
   * <ul>
   *   <li>early firings 30s of processing time after the first element (fresh dashboards),
   *   <li>an on-time firing when the watermark passes the end of the window,
   *   <li>one late firing per late element for up to {@code allowedLateness},
   *   <li>accumulating panes so every emitted row is a complete running total for the window.
   * </ul>
   */
  static PCollection<TableRow> computeStoreMetrics(
      PCollection<Received<OrderEvent>> orders, Duration windowSize, Duration allowedLateness) {
    return orders
        .apply("OrderCreatedByStore", ParDo.of(new KeyCreatedOrdersByStoreFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(OrderEvent.class)))
        .apply(
            "MetricsWindow",
            Window.<KV<String, OrderEvent>>into(FixedWindows.of(windowSize))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(30)))
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(allowedLateness)
                .accumulatingFiredPanes())
        .apply("CombineStoreMetrics", Combine.perKey(new StoreMetricsCombineFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(StoreMetrics.class)))
        .apply("StoreMetricsToRows", ParDo.of(new StoreMetricsToTableRowFn()))
        .setCoder(TableRowJsonCoder.of());
  }

  /** Keeps ORDER_CREATED events and keys them by {@code order.storeId}. */
  static final class KeyCreatedOrdersByStoreFn
      extends DoFn<Received<OrderEvent>, KV<String, OrderEvent>> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(
        @Element Received<OrderEvent> received, OutputReceiver<KV<String, OrderEvent>> out) {
      OrderEvent event = received.payload();
      if (event.orderCreated() && event.order() != null && event.order().storeId() != null) {
        out.output(KV.of(event.order().storeId(), event));
      }
    }
  }

  private static PCollection<DeadLetter> writeBigQuery(
      PCollection<TableRow> rows,
      TableReference table,
      TableSchema schema,
      String partitionField,
      List<String> clusterFields,
      boolean atLeastOnce,
      Duration frequency) {
    String name = table.getTableId();
    WriteResult result =
        rows.apply(
            "Write_" + name,
            BigQueryWrites.streaming(
                table, schema, partitionField, clusterFields, atLeastOnce, frequency));
    // The failed-insert output inherits BigQueryIO's internal windowing/trigger; Flatten requires
    // all inputs to have compatible WindowFns and triggers, so normalize to global/default first.
    return result
        .getFailedStorageApiInserts()
        .apply("FailedInserts_" + name, ParDo.of(new FailedInsertToDeadLetterFn(name)))
        .setCoder(SerializableCoder.of(DeadLetter.class))
        .apply(
            "RewindowFailedInserts_" + name,
            Window.<DeadLetter>into(new GlobalWindows())
                .triggering(DefaultTrigger.of())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes());
  }

  private static void writeLocal(PCollection<TableRow> rows, String dir, String table) {
    rows.apply("WriteLocal_" + table, ParDo.of(new LocalJsonLinesWriteFn(dir, table)));
  }
}
