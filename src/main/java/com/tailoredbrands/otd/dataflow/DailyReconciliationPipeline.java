package com.tailoredbrands.otd.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.bigquery.BigQuerySchemas;
import com.tailoredbrands.otd.dataflow.bigquery.BigQueryWrites;
import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import com.tailoredbrands.otd.dataflow.model.PubsubOrderSummary;
import com.tailoredbrands.otd.dataflow.model.ReconciliationResult;
import com.tailoredbrands.otd.dataflow.transforms.JsonLineToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.LegacyOrderToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.LegacyXmlParseFn;
import com.tailoredbrands.otd.dataflow.transforms.OrderEventRowToSummaryFn;
import com.tailoredbrands.otd.dataflow.transforms.ReconcileFn;
import com.tailoredbrands.otd.dataflow.transforms.ReconciliationToTableRowFn;
import com.tailoredbrands.otd.dataflow.transforms.TableRowToJsonLineFn;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch pipeline launched daily by Cloud Scheduler / Composer: joins the legacy OMS XML extract
 * for {@code runDate} with the ORDER_CREATED events Dataflow landed in {@code otd.order_events}
 * and classifies every order id.
 *
 * <pre>
 *  gs://.../oms-extract/<date>/*.xml ─▶ FileIO.match/readMatches ─▶ DOM parse ─▶ LegacyOrder ─┬▶ legacy_oms_orders (BQ)
 *                                                                                              │
 *  otd.order_events WHERE DATE(event_time)=runDate ─▶ PubsubOrderSummary ─┐                    ▼
 *                                                                        └──▶ CoGroupByKey(orderId) ─▶ ReconcileFn
 *                                                                                                       ├▶ order_reconciliation (BQ)
 *                                                                                                       └▶ Count.perElement ─▶ summary CSV (GCS)
 * </pre>
 */
public final class DailyReconciliationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DailyReconciliationPipeline.class);

  static final TupleTag<LegacyOrder> LEGACY_TAG = new TupleTag<LegacyOrder>() {};
  static final TupleTag<PubsubOrderSummary> PUBSUB_TAG = new TupleTag<PubsubOrderSummary>() {};

  /** The interesting PCollections, exposed so tests can {@code PAssert} on them. */
  public record Outputs(
      PCollection<LegacyOrder> legacyOrders,
      PCollection<ReconciliationResult> results,
      PCollection<String> summaryLines) {}

  private DailyReconciliationPipeline() {}

  public static void main(String[] args) {
    PipelineOptionsFactory.register(DailyReconciliationOptions.class);
    DailyReconciliationOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DailyReconciliationOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    build(pipeline, options);
    pipeline.run().waitUntilFinish();
  }

  /** Assembles the DAG; returns the PCollections tests assert on. */
  public static Outputs build(Pipeline pipeline, DailyReconciliationOptions options) {
    String runDate = validateRunDate(options.getRunDate());
    boolean localIn = notBlank(options.getLocalInputDir());
    boolean localOut = notBlank(options.getLocalOutputDir());

    String legacyPath = options.getLegacyExtractPath();
    if (!notBlank(legacyPath)) {
      if (!localIn) {
        throw new IllegalArgumentException("--legacyExtractPath is required unless --localInputDir is set");
      }
      legacyPath = options.getLocalInputDir() + "/legacy/*.xml";
    }
    LOG.info("Reconciling runDate={} legacyExtractPath={} localIn={} localOut={}", runDate, legacyPath, localIn, localOut);

    // 1. Legacy OMS extract: match files -> read -> DOM parse
    PCollection<LegacyOrder> legacyOrders =
        pipeline
            .apply(
                "MatchLegacyExtract",
                FileIO.match()
                    .filepattern(legacyPath)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
            .apply("ReadLegacyExtract", FileIO.readMatches())
            .apply("ParseLegacyXml", ParDo.of(new LegacyXmlParseFn()))
            .setCoder(SerializableCoder.of(LegacyOrder.class));

    // 2. order_events for the run date: BigQuery (prod) or JSON lines (local/CI)
    PCollection<TableRow> orderEventRows;
    if (localIn) {
      orderEventRows =
          pipeline
              .apply(
                  "ReadOrderEventsJsonl",
                  TextIO.read().from(options.getLocalInputDir() + "/order_events.jsonl"))
              .apply("JsonLinesToRows", ParDo.of(new JsonLineToTableRowFn()))
              .setCoder(TableRowJsonCoder.of());
    } else {
      String project = bigQueryProject(options);
      String query =
          String.format(
              "SELECT order_id, store_id, event_type, "
                  + "FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E3SZ', event_time) AS event_time, "
                  + "total_amount, line_count "
                  + "FROM `%s.%s.%s` "
                  + "WHERE event_type = 'ORDER_CREATED' AND DATE(event_time) = '%s'",
              project, options.getBigQueryDataset(), BigQuerySchemas.ORDER_EVENTS, runDate);
      orderEventRows =
          pipeline.apply(
              "ReadOrderEventsBigQuery",
              BigQueryIO.readTableRows().fromQuery(query).usingStandardSql());
    }

    // 3. Key both sides by order id and join
    PCollection<KV<String, LegacyOrder>> legacyByOrderId =
        legacyOrders
            .apply("KeyLegacyByOrderNbr", ParDo.of(new KeyLegacyOrderFn()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(LegacyOrder.class)));
    PCollection<KV<String, PubsubOrderSummary>> pubsubByOrderId =
        orderEventRows
            .apply("SummarizeOrderEvents", ParDo.of(new OrderEventRowToSummaryFn()))
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(PubsubOrderSummary.class)));

    PCollection<KV<String, CoGbkResult>> joined =
        KeyedPCollectionTuple.of(LEGACY_TAG, legacyByOrderId)
            .and(PUBSUB_TAG, pubsubByOrderId)
            .apply("JoinOnOrderId", CoGroupByKey.create());

    // 4. Classify
    double tolerance = options.getAmountTolerance() == null ? 0.01 : options.getAmountTolerance();
    PCollection<ReconciliationResult> results =
        joined
            .apply("Classify", ParDo.of(new ReconcileFn(LEGACY_TAG, PUBSUB_TAG, runDate, tolerance)))
            .setCoder(SerializableCoder.of(ReconciliationResult.class));

    // 5. Summary CSV: run_date,classification,count
    PCollection<String> summaryLines =
        results
            .apply(
                "ClassificationOnly",
                MapElements.into(TypeDescriptors.strings())
                    .via((ReconciliationResult r) -> r.classification()))
            .apply("CountPerClassification", Count.perElement())
            .apply(
                "FormatSummaryLine",
                MapElements.into(TypeDescriptors.strings())
                    .via((KV<String, Long> kv) -> runDate + "," + kv.getKey() + "," + kv.getValue()));
    summaryLines.apply(
        "WriteSummaryCsv",
        TextIO.write()
            .to(options.getReportGcsPath())
            .withoutSharding()
            .withHeader("run_date,classification,count"));

    // 6. Detail tables
    String runId = options.getJobName() == null ? "local" : options.getJobName();
    PCollection<TableRow> legacyRows =
        legacyOrders
            .apply("LegacyOrdersToRows", ParDo.of(new LegacyOrderToTableRowFn(runDate)))
            .setCoder(TableRowJsonCoder.of());
    PCollection<TableRow> reconciliationRows =
        results
            .apply("ReconciliationToRows", ParDo.of(new ReconciliationToTableRowFn(runId)))
            .setCoder(TableRowJsonCoder.of());

    if (localOut) {
      writeLocal(legacyRows, options.getLocalOutputDir(), BigQuerySchemas.LEGACY_OMS_ORDERS);
      writeLocal(reconciliationRows, options.getLocalOutputDir(), BigQuerySchemas.ORDER_RECONCILIATION);
    } else {
      String project = bigQueryProject(options);
      String dataset = options.getBigQueryDataset();
      legacyRows.apply(
          "WriteLegacyOmsOrders",
          BigQueryWrites.batch(
              BigQueryWrites.table(project, dataset, BigQuerySchemas.LEGACY_OMS_ORDERS),
              BigQuerySchemas.legacyOmsOrders(),
              "extract_date",
              List.of("store_nbr")));
      reconciliationRows.apply(
          "WriteOrderReconciliation",
          BigQueryWrites.batch(
              BigQueryWrites.table(project, dataset, BigQuerySchemas.ORDER_RECONCILIATION),
              BigQuerySchemas.orderReconciliation(),
              "run_date",
              List.of("classification")));
    }
    return new Outputs(legacyOrders, results, summaryLines);
  }

  /** Keys legacy orders by {@code OrderNbr} (same identifier space as {@code order.orderId}). */
  static final class KeyLegacyOrderFn extends DoFn<LegacyOrder, KV<String, LegacyOrder>> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(@Element LegacyOrder order, OutputReceiver<KV<String, LegacyOrder>> out) {
      if (order.orderNbr() != null && !order.orderNbr().isBlank()) {
        out.output(KV.of(order.orderNbr().trim(), order));
      }
    }
  }

  private static void writeLocal(PCollection<TableRow> rows, String dir, String table) {
    rows.apply("RowsToJson_" + table, ParDo.of(new TableRowToJsonLineFn()))
        .apply(
            "WriteLocal_" + table,
            TextIO.write().to(dir + "/" + table).withSuffix(".jsonl").withoutSharding());
  }

  static String validateRunDate(String runDate) {
    if (runDate == null || runDate.isBlank()) {
      // Cloud Scheduler launches the template with a static body, so "yesterday (UTC)" is the default.
      return LocalDate.now(java.time.ZoneOffset.UTC).minusDays(1).toString();
    }
    try {
      return LocalDate.parse(runDate).toString();
    } catch (DateTimeParseException | NullPointerException e) {
      throw new IllegalArgumentException("--runDate must be yyyy-MM-dd, got '" + runDate + "'", e);
    }
  }

  private static String bigQueryProject(DailyReconciliationOptions options) {
    return notBlank(options.getBigQueryProject()) ? options.getBigQueryProject() : options.getProject();
  }

  private static boolean notBlank(String s) {
    return s != null && !s.isBlank();
  }
}
