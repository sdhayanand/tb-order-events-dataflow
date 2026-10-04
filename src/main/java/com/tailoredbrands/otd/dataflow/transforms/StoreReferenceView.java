package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.StoreRef;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Builds the {@code storeId -> StoreRef} side input from an optional CSV
 * ({@code store_id,store_name,region,timezone}) on GCS or the local file system. When no path is
 * configured an empty map is used so the enrichment step still runs (with null store name/region).
 *
 * <p>The CSV is a bounded source read once at job start; in a streaming job the global-window side
 * input becomes available as soon as the bounded read completes. Reference data that changes while
 * the job runs would need a slowly-changing side input (periodic {@code GenerateSequence} +
 * re-read), which is discussed in the README.
 */
public final class StoreReferenceView {

  private StoreReferenceView() {}

  public static PCollectionView<Map<String, StoreRef>> build(Pipeline pipeline, String csvPath) {
    PCollection<KV<String, StoreRef>> entries;
    if (csvPath == null || csvPath.isBlank()) {
      entries =
          pipeline.apply(
              "EmptyStoreReference",
              Create.empty(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(StoreRef.class))));
    } else {
      entries =
          pipeline
              .apply("ReadStoreReferenceCsv", TextIO.read().from(csvPath))
              .apply("ParseStoreReferenceCsv", ParDo.of(new ParseCsvLineFn()))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(StoreRef.class)));
    }
    return entries.apply("StoreReferenceAsMap", View.asMap());
  }

  /** Parses {@code store_id,store_name,region,timezone}; skips the header and blank lines. */
  static final class ParseCsvLineFn extends DoFn<String, KV<String, StoreRef>> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, StoreRef>> out) {
      if (line == null || line.isBlank() || line.toLowerCase().startsWith("store_id")) {
        return;
      }
      String[] cols = line.split(",", -1);
      if (cols.length < 2 || cols[0].isBlank()) {
        return;
      }
      String storeId = cols[0].trim();
      String storeName = cols[1].trim();
      String region = cols.length > 2 ? cols[2].trim() : null;
      String timezone = cols.length > 3 ? cols[3].trim() : null;
      out.output(KV.of(storeId, new StoreRef(storeId, storeName, region, timezone)));
    }
  }
}
