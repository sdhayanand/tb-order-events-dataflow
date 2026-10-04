package com.tailoredbrands.otd.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.TableRow;
import com.tailoredbrands.otd.dataflow.model.Alteration;
import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.model.Order;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderLine;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.model.Rental;
import com.tailoredbrands.otd.dataflow.util.Json;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Drives the parse + windowed-metrics part of the streaming DAG with a {@link TestStream} of
 * Pub/Sub messages whose timestamps span two one-minute windows, plus one late element, and
 * asserts the {@code store_order_metrics} rows per window and pane.
 */
@RunWith(JUnit4.class)
public class OrderEventsStreamingPipelineTest {

  private static final Instant BASE = Instant.parse("2026-10-03T22:00:00.000Z");
  private static final Duration WINDOW = Duration.standardMinutes(1);
  private static final IntervalWindow WINDOW_1 = new IntervalWindow(BASE, BASE.plus(WINDOW));
  private static final IntervalWindow WINDOW_2 =
      new IntervalWindow(BASE.plus(WINDOW), BASE.plus(WINDOW).plus(WINDOW));

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void metricsPerStoreAndWindowWithOnTimeAndLatePanes() {
    TestStream<PubsubMessage> stream =
        TestStream.create(PubsubMessageWithAttributesCoder.of())
            .advanceWatermarkTo(BASE)
            // window 1: two stores, one ORDER_UPDATED that must be ignored by the metrics
            .addElements(
                at(order("ORD-1", "0412", "TAILORED", 649.99, 1, false), BASE.plus(seconds(10))),
                at(order("ORD-2", "0412", "RENTAL", 219.00, 0, true), BASE.plus(seconds(20))),
                at(order("ORD-3", "0199", "RETAIL", 159.00, 0, false), BASE.plus(seconds(30))),
                at(updated("ORD-1", "0412", 649.99), BASE.plus(seconds(40))))
            // watermark passes the end of window 1 -> ON_TIME pane for window 1
            .advanceWatermarkTo(BASE.plus(seconds(90)))
            // window 2 element, and a LATE element for window 1 (within the 5 min allowed lateness)
            .addElements(
                at(order("ORD-4", "0412", "RETAIL", 100.00, 0, false), BASE.plus(seconds(75))),
                at(order("ORD-5", "0199", "RETAIL", 40.00, 0, false), BASE.plus(seconds(50))))
            .advanceWatermarkToInfinity();

    PCollection<PubsubMessage> messages = pipeline.apply(stream);
    PCollectionTuple parsed = OrderEventsStreamingPipeline.parseOrders(messages);
    PCollection<Received<OrderEvent>> orders = parsed.get(OrderEventsStreamingPipeline.ORDERS_TAG);
    PCollection<DeadLetter> deadLetters = parsed.get(OrderEventsStreamingPipeline.DEAD_LETTER_TAG);
    PCollection<TableRow> metrics =
        OrderEventsStreamingPipeline.computeStoreMetrics(
            orders, WINDOW, Duration.standardMinutes(5));

    PAssert.that(deadLetters).empty();

    PAssert.that(metrics)
        .inOnTimePane(WINDOW_1)
        .satisfies(
            rows -> {
              List<TableRow> list = toList(rows);
              assertEquals("two stores in window 1 on-time pane: " + list, 2, list.size());
              expect(list, "0412", 2, 868.99, 1, 1, "ON_TIME", WINDOW_1);
              expect(list, "0199", 1, 159.00, 0, 0, "ON_TIME", WINDOW_1);
              return null;
            });

    PAssert.that(metrics)
        .inLatePane(WINDOW_1)
        .satisfies(
            rows -> {
              List<TableRow> list = toList(rows);
              assertEquals("only store 0199 received late data: " + list, 1, list.size());
              // accumulating panes: the late row is the complete running total for the window
              expect(list, "0199", 2, 199.00, 0, 0, "LATE", WINDOW_1);
              return null;
            });

    PAssert.that(metrics)
        .inOnTimePane(WINDOW_2)
        .satisfies(
            rows -> {
              List<TableRow> list = toList(rows);
              assertEquals("one store in window 2: " + list, 1, list.size());
              expect(list, "0412", 1, 100.00, 0, 0, "ON_TIME", WINDOW_2);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void invalidMessagesOnlyReachTheDeadLetterOutput() {
    TestStream<PubsubMessage> stream =
        TestStream.create(PubsubMessageWithAttributesCoder.of())
            .advanceWatermarkTo(BASE)
            .addElements(
                at(order("ORD-1", "0412", "RETAIL", 10.0, 0, false), BASE.plus(seconds(1))),
                at(raw("{\"eventType\":\"ORDER_CREATED\"}"), BASE.plus(seconds(2))),
                at(raw("garbage"), BASE.plus(seconds(3))))
            .advanceWatermarkToInfinity();

    PCollectionTuple parsed =
        OrderEventsStreamingPipeline.parseOrders(pipeline.apply(stream));

    PAssert.that(parsed.get(OrderEventsStreamingPipeline.ORDERS_TAG))
        .satisfies(
            received -> {
              assertEquals(1, toList(received).size());
              return null;
            });
    PAssert.that(parsed.get(OrderEventsStreamingPipeline.DEAD_LETTER_TAG))
        .satisfies(
            deadLetters -> {
              List<DeadLetter> list = toList(deadLetters);
              assertEquals(2, list.size());
              for (DeadLetter d : list) {
                assertEquals("orders-v1", d.originalTopic());
                assertNotNull(d.reason());
              }
              return null;
            });
    pipeline.run();
  }

  // ------------------------------------------------------------------ helpers

  private static Duration seconds(long s) {
    return Duration.standardSeconds(s);
  }

  private static TimestampedValue<PubsubMessage> at(PubsubMessage m, Instant ts) {
    return TimestampedValue.of(m, ts);
  }

  private static PubsubMessage raw(String payload) {
    return new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), Map.of());
  }

  private static PubsubMessage order(
      String orderId, String storeId, String orderType, double total, int alterationLines, boolean rental) {
    List<OrderLine> lines = new ArrayList<>();
    lines.add(new OrderLine(1, "SKU-MAIN", 1, total, "STORE_PICKUP", null));
    for (int i = 0; i < alterationLines; i++) {
      lines.add(new OrderLine(2 + i, "ALT-" + i, 1, 0.0, "ALTERATION", new Alteration("HEM", 31.5, "TS-1")));
    }
    Order order =
        new Order(orderId, orderType, "STORE", storeId, "C-1", "2026-10-03T22:00:00Z", "2026-10-10",
            "USD", total, lines, rental ? new Rental("WED-1", "2026-10-18", "2026-10-20", null) : null, null);
    OrderEvent event =
        new OrderEvent("evt-" + orderId, "ORDER_CREATED", "2026-10-03T22:00:00Z", "1",
            "ORDER_INTAKE_API", "corr-" + orderId, null, order);
    return new PubsubMessage(
        Json.write(event).getBytes(StandardCharsets.UTF_8),
        Map.of("eventType", "ORDER_CREATED", "storeId", storeId, "schemaVersion", "1"));
  }

  private static PubsubMessage updated(String orderId, String storeId, double total) {
    Order order =
        new Order(orderId, "TAILORED", "STORE", storeId, "C-1", "2026-10-03T22:00:00Z", "2026-10-10",
            "USD", total, List.of(new OrderLine(1, "SKU-MAIN", 1, total, "STORE_PICKUP", null)), null, null);
    OrderEvent event =
        new OrderEvent("evt-upd-" + orderId, "ORDER_UPDATED", "2026-10-03T22:00:30Z", "1",
            "ORDER_INTAKE_API", "corr-" + orderId, null, order);
    return new PubsubMessage(
        Json.write(event).getBytes(StandardCharsets.UTF_8), Map.of("eventType", "ORDER_UPDATED"));
  }

  private static <T> List<T> toList(Iterable<T> iterable) {
    List<T> list = new ArrayList<>();
    iterable.forEach(list::add);
    return list;
  }

  private static void expect(
      List<TableRow> rows,
      String storeId,
      long orders,
      double revenue,
      long alterationLines,
      long rentalOrders,
      String paneTiming,
      IntervalWindow window) {
    for (TableRow row : rows) {
      if (storeId.equals(String.valueOf(row.get("store_id")))) {
        assertEquals("orders for " + storeId, orders, ((Number) row.get("orders")).longValue());
        assertEquals("revenue for " + storeId, revenue, ((Number) row.get("revenue")).doubleValue(), 0.001);
        assertEquals("alteration_lines for " + storeId, alterationLines,
            ((Number) row.get("alteration_lines")).longValue());
        assertEquals("rental_orders for " + storeId, rentalOrders,
            ((Number) row.get("rental_orders")).longValue());
        assertEquals(paneTiming, row.get("pane_timing"));
        assertEquals(Times.toIso(window.start()), row.get("window_start"));
        assertEquals(Times.toIso(window.end()), row.get("window_end"));
        return;
      }
    }
    fail("no metrics row for store " + storeId + " in " + rows);
  }
}
