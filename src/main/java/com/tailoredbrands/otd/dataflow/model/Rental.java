package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Tuxedo-rental block of an order (event date, wedding-party linkage, return logistics). The
 * contract leaves this open-ended, so unknown properties are ignored by the JSON mapper and the
 * whole block is persisted as a JSON string in BigQuery.
 */
@DefaultCoder(SerializableCoder.class)
public record Rental(String eventId, String eventDate, String returnDueDate, String groupId)
    implements Serializable {

  private static final long serialVersionUID = 1L;
}
