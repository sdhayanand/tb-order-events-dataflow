package com.tailoredbrands.otd.dataflow.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/** Small RFC-3339 helpers used by validators and row builders. */
public final class Times {

  private Times() {}

  /** True when {@code value} is an RFC-3339 timestamp ({@code 2026-10-03T22:14:05.120Z}, offsets allowed). */
  public static boolean isRfc3339(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    try {
      OffsetDateTime.parse(value);
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  /** True when {@code value} is an ISO date ({@code 2026-10-10}). */
  public static boolean isIsoDate(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    try {
      LocalDate.parse(value);
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  /** Current wall-clock time as an RFC-3339 string in UTC. */
  public static String nowIso() {
    return Instant.now().toString();
  }

  /** Joda instant (Beam's element timestamp) to RFC-3339 UTC string. */
  public static String toIso(org.joda.time.Instant instant) {
    return Instant.ofEpochMilli(instant.getMillis()).toString();
  }
}
