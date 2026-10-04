package com.tailoredbrands.otd.dataflow.transforms;

import com.google.api.services.bigquery.model.TableRow;

/** Tiny helpers for building {@link TableRow}s without writing explicit nulls. */
final class Rows {

  private Rows() {}

  /** Sets {@code name} only when {@code value} is non-null (NULLABLE columns stay absent). */
  static TableRow put(TableRow row, String name, Object value) {
    if (value != null) {
      row.set(name, value);
    }
    return row;
  }

  /** Converts an arbitrary BigQuery cell (Number / String / null) to a double, defaulting to 0. */
  static double toDouble(Object value) {
    if (value == null) {
      return 0.0;
    }
    if (value instanceof Number n) {
      return n.doubleValue();
    }
    String s = String.valueOf(value).trim();
    return s.isEmpty() ? 0.0 : Double.parseDouble(s);
  }

  /** Converts an arbitrary BigQuery cell (Number / String / null) to a long, defaulting to 0. */
  static long toLong(Object value) {
    if (value == null) {
      return 0L;
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    String s = String.valueOf(value).trim();
    return s.isEmpty() ? 0L : (long) Double.parseDouble(s);
  }

  static String toStringOrNull(Object value) {
    return value == null ? null : String.valueOf(value);
  }
}
