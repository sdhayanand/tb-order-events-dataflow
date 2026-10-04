package com.tailoredbrands.otd.dataflow.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Mutable accumulator for the per-store metrics combine. Kept deliberately tiny: four numbers,
 * Java-serializable, cheap to merge. Mutability is fine for a Beam accumulator as long as
 * {@code addInput}/{@code mergeAccumulators} return the instance they mutated.
 */
public final class StoreMetricsAccumulator implements Serializable {

  private static final long serialVersionUID = 1L;

  public long orders;
  public double revenue;
  public long alterationLines;
  public long rentalOrders;

  public StoreMetricsAccumulator() {}

  public StoreMetricsAccumulator(long orders, double revenue, long alterationLines, long rentalOrders) {
    this.orders = orders;
    this.revenue = revenue;
    this.alterationLines = alterationLines;
    this.rentalOrders = rentalOrders;
  }

  public StoreMetricsAccumulator add(Order order) {
    orders += 1;
    revenue += order.totalAmount() == null ? 0.0 : order.totalAmount();
    alterationLines += order.alterationLineCount();
    if (order.countsAsRental()) {
      rentalOrders += 1;
    }
    return this;
  }

  public StoreMetricsAccumulator merge(StoreMetricsAccumulator other) {
    orders += other.orders;
    revenue += other.revenue;
    alterationLines += other.alterationLines;
    rentalOrders += other.rentalOrders;
    return this;
  }

  public StoreMetrics toMetrics() {
    return new StoreMetrics(orders, revenue, alterationLines, rentalOrders);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoreMetricsAccumulator that)) {
      return false;
    }
    return orders == that.orders
        && Double.compare(revenue, that.revenue) == 0
        && alterationLines == that.alterationLines
        && rentalOrders == that.rentalOrders;
  }

  @Override
  public int hashCode() {
    return Objects.hash(orders, revenue, alterationLines, rentalOrders);
  }

  @Override
  public String toString() {
    return "StoreMetricsAccumulator{orders=" + orders + ", revenue=" + revenue
        + ", alterationLines=" + alterationLines + ", rentalOrders=" + rentalOrders + '}';
  }
}
