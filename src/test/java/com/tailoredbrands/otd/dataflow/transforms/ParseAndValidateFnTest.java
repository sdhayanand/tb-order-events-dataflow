package com.tailoredbrands.otd.dataflow.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.tailoredbrands.otd.dataflow.TestResources;
import com.tailoredbrands.otd.dataflow.model.DeadLetter;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.Received;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParseAndValidateFnTest {

  private static final TupleTag<Received<OrderEvent>> MAIN = new TupleTag<Received<OrderEvent>>() {};
  private static final TupleTag<DeadLetter> DLQ = new TupleTag<DeadLetter>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private PCollectionTuple parse(PubsubMessage... messages) {
    PCollection<PubsubMessage> input =
        pipeline.apply(
            Create.of(List.of(messages))
                .withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()));
    PCollectionTuple out =
        input.apply(
            ParDo.of(
                    new ParseAndValidateFn<>(
                        OrderEvent.class, new OrderEventValidator(), "orders", "orders-v1", DLQ))
                .withOutputTags(MAIN, TupleTagList.of(DLQ)));
    out.get(MAIN).setCoder(SerializableCoder.of(new TypeDescriptor<Received<OrderEvent>>() {}));
    out.get(DLQ).setCoder(SerializableCoder.of(DeadLetter.class));
    return out;
  }

  private static PubsubMessage message(String resource, String messageId) {
    return new PubsubMessage(
        TestResources.bytes(resource),
        Map.of("eventType", "ORDER_CREATED", "schemaVersion", "1", "storeId", "0412"),
        messageId);
  }

  @Test
  public void validOrderCreatedGoesToMainOutput() {
    PCollectionTuple out = parse(message("samples/order-created.json", "m-1"));

    PAssert.that(out.get(MAIN))
        .satisfies(
            received -> {
              List<Received<OrderEvent>> list = toList(received);
              assertEquals(1, list.size());
              Received<OrderEvent> r = list.get(0);
              assertEquals("m-1", r.messageId());
              assertEquals("ORD-2026-000123", r.payload().order().orderId());
              assertEquals(2, r.payload().order().lines().size());
              assertEquals("0412", r.attributes().get("storeId"));
              assertNotNull(r.publishTime());
              return null;
            });
    PAssert.that(out.get(DLQ)).empty();
    pipeline.run();
  }

  @Test
  public void invalidEnumGoesToDeadLetterWithValidateStage() {
    PCollectionTuple out = parse(message("samples/order-invalid-enum.json", "m-2"));

    PAssert.that(out.get(MAIN)).empty();
    PAssert.that(out.get(DLQ))
        .satisfies(
            deadLetters -> {
              List<DeadLetter> list = toList(deadLetters);
              assertEquals(1, list.size());
              DeadLetter d = list.get(0);
              assertEquals(DeadLetter.STAGE_VALIDATE, d.stage());
              assertEquals("orders-v1", d.originalTopic());
              assertEquals("m-2", d.messageId());
              assertTrue(d.reason(), d.reason().contains("eventType 'ORDER_PLACED'"));
              assertTrue(d.payload().contains("ORD-2026-000999"));
              assertEquals("0412", d.attributes().get("storeId"));
              return null;
            });
    pipeline.run();
  }

  @Test
  public void missingRequiredFieldsAndEmptyLinesAreReported() {
    String json =
        "{\"eventId\":\"e-1\",\"eventType\":\"ORDER_CREATED\",\"eventTime\":\"2026-10-03T22:14:05Z\","
            + "\"schemaVersion\":\"1\",\"source\":\"ORDER_INTAKE_API\","
            + "\"order\":{\"orderId\":\"ORD-1\",\"orderType\":\"RETAIL\",\"channel\":\"STORE\","
            + "\"currency\":\"USD\",\"totalAmount\":-5,\"lines\":[]}}";
    PubsubMessage msg =
        new PubsubMessage(json.getBytes(StandardCharsets.UTF_8), Map.of(), "m-3");
    PCollectionTuple out = parse(msg);

    PAssert.that(out.get(MAIN)).empty();
    PAssert.that(out.get(DLQ))
        .satisfies(
            deadLetters -> {
              List<DeadLetter> list = toList(deadLetters);
              assertEquals(1, list.size());
              String reason = list.get(0).reason();
              assertTrue(reason, reason.contains("order.storeId is required"));
              assertTrue(reason, reason.contains("totalAmount must be >= 0"));
              assertTrue(reason, reason.contains("lines must not be empty for ORDER_CREATED"));
              return null;
            });
    pipeline.run();
  }

  @Test
  public void malformedJsonGoesToDeadLetterWithParseStage() {
    PubsubMessage msg =
        new PubsubMessage("{not json".getBytes(StandardCharsets.UTF_8), Map.of(), "m-4");
    PCollectionTuple out = parse(msg);

    PAssert.that(out.get(MAIN)).empty();
    PAssert.that(out.get(DLQ))
        .satisfies(
            deadLetters -> {
              List<DeadLetter> list = toList(deadLetters);
              assertEquals(1, list.size());
              assertEquals(DeadLetter.STAGE_PARSE, list.get(0).stage());
              assertEquals("{not json", list.get(0).payload());
              return null;
            });
    pipeline.run();
  }

  @Test
  public void mixedBatchSplitsCorrectly() {
    PCollectionTuple out =
        parse(
            message("samples/order-created.json", "ok-1"),
            message("samples/order-created-rental.json", "ok-2"),
            message("samples/order-cancelled.json", "ok-3"),
            message("samples/order-invalid-enum.json", "bad-1"));

    PAssert.that(out.get(MAIN))
        .satisfies(
            received -> {
              assertEquals(3, toList(received).size());
              return null;
            });
    PAssert.that(out.get(DLQ))
        .satisfies(
            deadLetters -> {
              assertEquals(1, toList(deadLetters).size());
              return null;
            });
    pipeline.run();
  }

  private static <T> List<T> toList(Iterable<T> iterable) {
    List<T> list = new ArrayList<>();
    iterable.forEach(list::add);
    return list;
  }
}
