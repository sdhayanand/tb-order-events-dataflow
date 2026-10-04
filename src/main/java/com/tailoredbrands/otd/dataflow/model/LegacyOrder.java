package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * One {@code <Order>} from the legacy OMS XML extract ({@code <Orders><Order>...</Order></Orders>},
 * element names from the order-intake XSD: OrderNbr, OrderType, StoreNbr, CustNbr, OrderDate,
 * Lines/Line/{LineNbr,SKU,Qty,Price,FulfillType}).
 */
@DefaultCoder(SerializableCoder.class)
public record LegacyOrder(
    String orderNbr,
    String orderType,
    String storeNbr,
    String custNbr,
    String orderDate,
    Double totalAmount,
    List<LegacyLine> lines,
    String sourceFile)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  public List<LegacyLine> safeLines() {
    return lines == null ? List.of() : lines;
  }

  /** The extract's own total when present, otherwise the sum of {@code Qty * Price} over lines. */
  public double effectiveTotal() {
    if (totalAmount != null) {
      return totalAmount;
    }
    return safeLines().stream().mapToDouble(LegacyLine::lineAmount).sum();
  }
}
