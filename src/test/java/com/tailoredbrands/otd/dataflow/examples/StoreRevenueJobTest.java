package com.tailoredbrands.otd.dataflow.examples;

import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Runs the job's transforms on the DirectRunner with in-memory input: no GCP, a few seconds. */
public class StoreRevenueJobTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void sumsRevenuePerStoreAndSkipsBadLines() {
    PCollection<String> lines =
        pipeline.apply(
            Create.of(
                List.of(
                    "order_id,store_id,amount",
                    "ORD-1,0412,599.99",
                    "ORD-2,0412,50.00",
                    "ORD-3,0875,79.50",
                    "ORD-4,0875,not-a-number",
                    "")));

    PAssert.that(StoreRevenueJob.revenuePerStore(lines))
        .containsInAnyOrder("0412,649.99", "0875,79.50");

    pipeline.run().waitUntilFinish();
  }
}
