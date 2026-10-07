package com.tailoredbrands.otd.dataflow.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * The smallest useful Dataflow job: total revenue per store from a CSV of orders.
 *
 * <pre>
 *  orders-sample.csv ─▶ ReadOrders ─▶ ParseOrders ─▶ SumPerStore ─▶ FormatCsv ─▶ WriteResults ─▶ store-revenue.csv
 *   (GCS, TextIO)       PCollection   ParDo(DoFn)    Sum.longsPerKey  MapElements   (GCS, TextIO)
 *                       &lt;String&gt;    KV&lt;store,cents&gt;
 * </pre>
 *
 * Batch (bounded input), so the job starts, processes every line and finishes. Run it with the
 * DirectRunner on a laptop or the DataflowRunner on GCP; the code does not change, only --runner.
 */
public final class StoreRevenueJob {

  /** Pipeline options = the job's command-line parameters (Beam generates the parsing). */
  public interface Options extends PipelineOptions {
    @Description("CSV of orders with header order_id,store_id,amount (local path or gs://...)")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Output file prefix, e.g. gs://bucket/hello/output/store-revenue")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  /**
   * DoFn = the per-element code. One CSV line in, zero or one KV(store, amount in cents) out.
   * Money is kept in whole cents (long) so sums are exact. Bad lines are counted, not fatal.
   */
  static class ParseOrderFn extends DoFn<String, KV<String, Long>> {
    private final Counter parsed = Metrics.counter("hello", "parsed_lines");
    private final Counter skipped = Metrics.counter("hello", "skipped_lines");

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, Long>> out) {
      if (line.isBlank() || line.startsWith("order_id")) {
        return; // header or empty line
      }
      String[] fields = line.split(",");
      try {
        String storeId = fields[1].trim();
        long cents = Math.round(Double.parseDouble(fields[2].trim()) * 100);
        out.output(KV.of(storeId, cents));
        parsed.inc();
      } catch (RuntimeException e) {
        skipped.inc(); // e.g. a missing column or "abc" as the amount
      }
    }
  }

  private StoreRevenueJob() {}

  /** The transforms between read and write, separate so a unit test can feed lines in memory. */
  static PCollection<String> revenuePerStore(PCollection<String> lines) {
    return lines
        .apply("ParseOrders", ParDo.of(new ParseOrderFn()))
        .apply("SumPerStore", Sum.longsPerKey())
        .apply(
            "FormatCsv",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "," + (kv.getValue() / 100) + "." + String.format("%02d", kv.getValue() % 100)));
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline.apply("ReadOrders", TextIO.read().from(options.getInput()));
    revenuePerStore(lines)
        .apply("WriteResults", TextIO.write().to(options.getOutput()).withSuffix(".csv").withoutSharding()
            .withHeader("store_id,revenue"));

    // Not a Flex Template launch, so blocking until the job finishes is fine here.
    pipeline.run().waitUntilFinish();
  }
}
