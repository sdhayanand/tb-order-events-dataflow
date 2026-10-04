package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.EnrichedOrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.Received;
import com.tailoredbrands.otd.dataflow.model.StoreRef;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Looks up {@code order.storeId} in the store-reference side input and attaches store name, region
 * and timezone. Unknown stores are not an error (the reference file may lag new store openings);
 * they are counted in the {@code otd.orders/store_not_found} metric and enriched with nulls.
 */
public class EnrichOrderFn extends DoFn<Received<OrderEvent>, EnrichedOrderEvent> {

  private static final long serialVersionUID = 1L;

  private final PCollectionView<Map<String, StoreRef>> storeView;
  private final Counter storeNotFound = Metrics.counter("otd.orders", "store_not_found");
  private final Counter enriched = Metrics.counter("otd.orders", "enriched");

  public EnrichOrderFn(PCollectionView<Map<String, StoreRef>> storeView) {
    this.storeView = storeView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Received<OrderEvent> received = c.element();
    OrderEvent event = received.payload();
    Map<String, StoreRef> stores = c.sideInput(storeView);
    String storeId = event.order() == null ? null : event.order().storeId();
    StoreRef ref = storeId == null ? null : stores.get(storeId);
    if (ref == null) {
      storeNotFound.inc();
    } else {
      enriched.inc();
    }
    c.output(
        new EnrichedOrderEvent(
            event,
            received.messageId(),
            received.publishTime(),
            ref == null ? null : ref.storeName(),
            ref == null ? null : ref.region(),
            ref == null ? null : ref.timezone()));
  }
}
