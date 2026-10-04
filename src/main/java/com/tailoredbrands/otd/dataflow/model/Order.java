package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Order header + lines carried inside {@link OrderEvent}. */
@DefaultCoder(SerializableCoder.class)
public record Order(
    String orderId,
    String orderType,
    String channel,
    String storeId,
    String customerId,
    String orderedAt,
    String promisedDate,
    String currency,
    Double totalAmount,
    List<OrderLine> lines,
    Rental rental,
    ShipTo shipTo)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Null-safe accessor; an absent {@code lines} array is treated as empty. */
  public List<OrderLine> safeLines() {
    return lines == null ? List.of() : lines;
  }

  /**
   * Named without a {@code get}/{@code is} prefix on purpose: Jackson would otherwise treat it as
   * a bean getter for a property called {@code rental} and clash with the record component.
   */
  public boolean countsAsRental() {
    return OrderTypes.RENTAL.equals(orderType) || rental != null;
  }

  public long alterationLineCount() {
    return safeLines().stream().filter(OrderLine::needsAlteration).count();
  }
}
