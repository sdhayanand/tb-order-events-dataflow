package com.tailoredbrands.otd.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Options of {@link DailyReconciliationPipeline}. Described for the Flex Template UI in
 * {@code metadata/daily-reconciliation-metadata.json}.
 */
public interface DailyReconciliationOptions extends DataflowPipelineOptions {

  @Description(
      "Glob of legacy OMS XML extract files, e.g. gs://bucket/oms-extract/2026-10-03/*.xml. "
          + "Defaults to <localInputDir>/legacy/*.xml when --localInputDir is set")
  String getLegacyExtractPath();

  void setLegacyExtractPath(String value);

  @Description("Business date to reconcile, yyyy-MM-dd (defaults to yesterday UTC when blank)")
  String getRunDate();

  void setRunDate(String value);

  @Description("BigQuery dataset holding order_events and receiving legacy_oms_orders / order_reconciliation")
  @Default.String("otd")
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @Description("BigQuery project; defaults to --project")
  String getBigQueryProject();

  void setBigQueryProject(String value);

  @Description("Full path of the CSV summary report, e.g. gs://bucket/reports/reconciliation-2026-10-03.csv")
  @Validation.Required
  String getReportGcsPath();

  void setReportGcsPath(String value);

  @Description("Local mode (DirectRunner): read <dir>/order_events.jsonl instead of BigQuery")
  String getLocalInputDir();

  void setLocalInputDir(String value);

  @Description("Local mode (DirectRunner): write <dir>/<table>.jsonl instead of BigQuery")
  String getLocalOutputDir();

  void setLocalOutputDir(String value);

  @Description("Absolute amount difference tolerated before an order is classified AMOUNT_MISMATCH")
  @Default.Double(0.01)
  Double getAmountTolerance();

  void setAmountTolerance(Double value);
}
