package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.util.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Local-only sink used when {@code --localOutputDir} is set with the DirectRunner (CI e2e against
 * the Pub/Sub emulator). Appends each row as one JSON line to {@code <dir>/<table>.jsonl}
 * immediately, so the files are complete even when the never-ending streaming job is killed.
 *
 * <p>This deliberately bypasses Beam's file sinks: {@code TextIO.write().withWindowedWrites()}
 * would only finalize files once windows close, which is awkward for an e2e harness that stops the
 * job with SIGTERM. Never use this on Dataflow (workers have no shared local disk).
 */
public class LocalJsonLinesWriteFn extends DoFn<TableRow, Void> {

  private static final long serialVersionUID = 1L;
  private static final Object LOCK = new Object();

  private final String outputDir;
  private final String table;
  private transient Path file;

  public LocalJsonLinesWriteFn(String outputDir, String table) {
    this.outputDir = outputDir;
    this.table = table;
  }

  @Setup
  public void setup() throws IOException {
    Path dir = Paths.get(outputDir);
    Files.createDirectories(dir);
    file = dir.resolve(table + ".jsonl");
  }

  @ProcessElement
  public void processElement(@Element TableRow row) throws IOException {
    String line = Json.writeRow(row) + System.lineSeparator();
    synchronized (LOCK) {
      Files.writeString(
          file,
          line,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    }
  }
}
