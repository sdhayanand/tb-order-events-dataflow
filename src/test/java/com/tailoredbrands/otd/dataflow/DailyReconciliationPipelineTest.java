package com.tailoredbrands.otd.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import com.tailoredbrands.otd.dataflow.model.ReconciliationResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs the whole batch DAG in local mode against the fixtures in
 * {@code src/test/resources/reconciliation}: 4 legacy orders, 4 Pub/Sub orders (one missing on
 * each side, one amount mismatch, one line-count mismatch, one match).
 */
@RunWith(JUnit4.class)
public class DailyReconciliationPipelineTest {

  private static final String RUN_DATE = "2026-10-03";
  private static final Path INPUT_DIR = Paths.get("src/test/resources/reconciliation").toAbsolutePath();
  private static final Path OUTPUT_DIR = Paths.get("target/reconciliation-test").toAbsolutePath();

  private static DailyReconciliationOptions options() {
    DailyReconciliationOptions o = PipelineOptionsFactory.create().as(DailyReconciliationOptions.class);
    o.setRunDate(RUN_DATE);
    o.setLocalInputDir(INPUT_DIR.toString());
    o.setLocalOutputDir(OUTPUT_DIR.toString());
    o.setReportGcsPath(OUTPUT_DIR.resolve("reconciliation-" + RUN_DATE + ".csv").toString());
    return o;
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(options());

  @Before
  public void cleanOutputDir() throws IOException {
    if (Files.exists(OUTPUT_DIR)) {
      try (Stream<Path> walk = Files.walk(OUTPUT_DIR)) {
        walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  @Test
  public void classifiesEveryOrderAndWritesLocalFiles() throws IOException {
    DailyReconciliationPipeline.Outputs outputs =
        DailyReconciliationPipeline.build(
            pipeline, pipeline.getOptions().as(DailyReconciliationOptions.class));

    PAssert.that(outputs.legacyOrders())
        .satisfies(
            legacy -> {
              List<LegacyOrder> list = toList(legacy);
              assertEquals(4, list.size());
              return null;
            });

    PAssert.that(outputs.results())
        .satisfies(
            results -> {
              Map<String, String> byOrder = new HashMap<>();
              Map<String, ReconciliationResult> full = new HashMap<>();
              for (ReconciliationResult r : results) {
                byOrder.put(r.orderId(), r.classification());
                full.put(r.orderId(), r);
              }
              assertEquals(5, byOrder.size());
              assertEquals(ReconciliationResult.MATCH, byOrder.get("ORD-2026-000001"));
              assertEquals(ReconciliationResult.AMOUNT_MISMATCH, byOrder.get("ORD-2026-000002"));
              assertEquals(ReconciliationResult.MISSING_IN_PUBSUB, byOrder.get("ORD-2026-000003"));
              assertEquals(ReconciliationResult.LINE_COUNT_MISMATCH, byOrder.get("ORD-2026-000004"));
              assertEquals(ReconciliationResult.MISSING_IN_LEGACY, byOrder.get("ORD-2026-000005"));

              ReconciliationResult mismatch = full.get("ORD-2026-000002");
              assertEquals(219.0, mismatch.legacyTotal(), 0.001);
              assertEquals(229.0, mismatch.pubsubTotal(), 0.001);
              assertEquals(-10.0, mismatch.amountDiff(), 0.001);

              ReconciliationResult lines = full.get("ORD-2026-000004");
              assertEquals(Long.valueOf(3), lines.legacyLineCount());
              assertEquals(Long.valueOf(2), lines.pubsubLineCount());

              assertEquals(RUN_DATE, full.get("ORD-2026-000001").runDate());
              return null;
            });

    PAssert.that(outputs.summaryLines())
        .containsInAnyOrder(
            RUN_DATE + ",MATCH,1",
            RUN_DATE + ",AMOUNT_MISMATCH,1",
            RUN_DATE + ",MISSING_IN_PUBSUB,1",
            RUN_DATE + ",LINE_COUNT_MISMATCH,1",
            RUN_DATE + ",MISSING_IN_LEGACY,1");

    pipeline.run().waitUntilFinish();

    Path summary = OUTPUT_DIR.resolve("reconciliation-" + RUN_DATE + ".csv");
    assertTrue("summary csv written: " + summary, Files.exists(summary));
    List<String> csv = Files.readAllLines(summary);
    assertEquals("run_date,classification,count", csv.get(0));
    assertEquals(6, csv.size());

    Path recon = OUTPUT_DIR.resolve("order_reconciliation.jsonl");
    assertTrue(Files.exists(recon));
    assertEquals(5, Files.readAllLines(recon).size());

    Path legacy = OUTPUT_DIR.resolve("legacy_oms_orders.jsonl");
    assertTrue(Files.exists(legacy));
    List<String> legacyLines = Files.readAllLines(legacy);
    assertEquals(4, legacyLines.size());
    assertTrue(legacyLines.get(0), legacyLines.get(0).contains("\"extract_date\":\"" + RUN_DATE + "\""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMalformedRunDate() {
    DailyReconciliationPipeline.validateRunDate("10/03/2026");
  }

  private static <T> List<T> toList(Iterable<T> iterable) {
    List<T> list = new ArrayList<>();
    iterable.forEach(list::add);
    return list;
  }
}
