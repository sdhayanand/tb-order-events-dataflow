package com.tailoredbrands.otd.dataflow.transforms;

import static org.junit.Assert.assertEquals;

import com.tailoredbrands.otd.dataflow.model.Alteration;
import com.tailoredbrands.otd.dataflow.model.Order;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderLine;
import com.tailoredbrands.otd.dataflow.model.Rental;
import com.tailoredbrands.otd.dataflow.model.StoreMetrics;
import com.tailoredbrands.otd.dataflow.model.StoreMetricsAccumulator;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StoreMetricsCombineFnTest {

  private final StoreMetricsCombineFn fn = new StoreMetricsCombineFn();

  static OrderEvent event(String orderId, String orderType, double total, List<OrderLine> lines, Rental rental) {
    Order order =
        new Order(orderId, orderType, "STORE", "0412", "C-1", "2026-10-03T22:14:00Z", "2026-10-10",
            "USD", total, lines, rental, null);
    return new OrderEvent("e-" + orderId, "ORDER_CREATED", "2026-10-03T22:14:05Z", "1",
        "ORDER_INTAKE_API", "corr", null, order);
  }

  static OrderLine line(int n, String sku, double price, String fulfillment) {
    Alteration alt = "ALTERATION".equals(fulfillment) ? new Alteration("HEM", 31.5, "TS-1") : null;
    return new OrderLine(n, sku, 1, price, fulfillment, alt);
  }

  @Test
  public void addInputAccumulatesAllFourMetrics() {
    StoreMetricsAccumulator acc = fn.createAccumulator();
    acc = fn.addInput(acc,
        event("ORD-1", "TAILORED", 649.99,
            List.of(line(1, "SUIT", 599.99, "STORE_PICKUP"), line(2, "HEM", 50.0, "ALTERATION")), null));
    acc = fn.addInput(acc,
        event("ORD-2", "RENTAL", 219.0,
            List.of(line(1, "TUX", 189.0, "RENTAL"), line(2, "SHOES", 30.0, "RENTAL")),
            new Rental("2026-10-18", "WED-1", "2026-10-20", null)));
    acc = fn.addInput(acc,
        event("ORD-3", "RETAIL", 100.0,
            List.of(line(1, "SHIRT", 50.0, "STORE_PICKUP"), line(2, "HEM-2", 25.0, "ALTERATION"),
                line(3, "HEM-3", 25.0, "ALTERATION")), null));

    StoreMetrics m = fn.extractOutput(acc);
    assertEquals(3, m.orders());
    assertEquals(968.99, m.revenue(), 0.0001);
    assertEquals(3, m.alterationLines());
    assertEquals(1, m.rentalOrders());
  }

  @Test
  public void mergeIsAssociativeAndDoesNotMutateInputs() {
    StoreMetricsAccumulator a = new StoreMetricsAccumulator(2, 100.0, 1, 0);
    StoreMetricsAccumulator b = new StoreMetricsAccumulator(3, 50.5, 0, 2);
    StoreMetricsAccumulator c = new StoreMetricsAccumulator(0, 0.0, 0, 0);

    StoreMetricsAccumulator merged = fn.mergeAccumulators(List.of(a, b, c));
    assertEquals(new StoreMetricsAccumulator(5, 150.5, 1, 2), merged);
    assertEquals(new StoreMetricsAccumulator(2, 100.0, 1, 0), a);
    assertEquals(new StoreMetricsAccumulator(3, 50.5, 0, 2), b);

    StoreMetricsAccumulator ab = fn.mergeAccumulators(List.of(fn.mergeAccumulators(List.of(a, b)), c));
    StoreMetricsAccumulator bc = fn.mergeAccumulators(List.of(a, fn.mergeAccumulators(List.of(b, c))));
    assertEquals(ab, bc);
  }

  @Test
  public void nullOrderAndNullTotalAreTolerated() {
    StoreMetricsAccumulator acc = fn.createAccumulator();
    acc = fn.addInput(acc, new OrderEvent("e", "ORDER_CREATED", "2026-10-03T22:14:05Z", "1",
        "ORDER_INTAKE_API", null, null, null));
    acc = fn.addInput(acc, event("ORD-9", "RETAIL", 0.0, List.of(), null));
    OrderEvent noTotal = new OrderEvent("e2", "ORDER_CREATED", "2026-10-03T22:14:05Z", "1",
        "ORDER_INTAKE_API", null, null,
        new Order("ORD-10", "RETAIL", "STORE", "0412", null, null, null, "USD", null, null, null, null));
    acc = fn.addInput(acc, noTotal);
    StoreMetrics m = fn.extractOutput(acc);
    assertEquals(2, m.orders());
    assertEquals(0.0, m.revenue(), 0.0);
  }

  @Test
  public void accumulatorCoderRoundTrips() throws Exception {
    Coder<StoreMetricsAccumulator> coder =
        fn.getAccumulatorCoder(CoderRegistry.createDefault(), SerializableCoder.of(OrderEvent.class));
    CoderProperties.coderDecodeEncodeEqual(coder, new StoreMetricsAccumulator(4, 12.5, 2, 1));
  }
}
