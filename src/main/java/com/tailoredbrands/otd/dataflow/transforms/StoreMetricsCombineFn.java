package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.StoreMetrics;
import com.tailoredbrands.otd.dataflow.model.StoreMetricsAccumulator;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Per-store, per-window aggregation: number of ORDER_CREATED events, revenue (sum of
 * {@code totalAmount}), alteration line count and rental order count.
 *
 * <p>Implemented as a {@link Combine.CombineFn} rather than a GroupByKey + loop so the runner can
 * pre-combine on the mapper side ("combiner lifting") and so accumulating panes stay cheap: with
 * {@code accumulatingFiredPanes()} the runner keeps only this tiny accumulator per key/window, not
 * the raw events.
 */
public class StoreMetricsCombineFn
    extends Combine.CombineFn<OrderEvent, StoreMetricsAccumulator, StoreMetrics> {

  private static final long serialVersionUID = 1L;

  @Override
  public StoreMetricsAccumulator createAccumulator() {
    return new StoreMetricsAccumulator();
  }

  @Override
  public StoreMetricsAccumulator addInput(StoreMetricsAccumulator accumulator, OrderEvent input) {
    if (input != null && input.order() != null) {
      accumulator.add(input.order());
    }
    return accumulator;
  }

  @Override
  public StoreMetricsAccumulator mergeAccumulators(Iterable<StoreMetricsAccumulator> accumulators) {
    StoreMetricsAccumulator merged = createAccumulator();
    for (StoreMetricsAccumulator a : accumulators) {
      merged.merge(a);
    }
    return merged;
  }

  @Override
  public StoreMetrics extractOutput(StoreMetricsAccumulator accumulator) {
    return accumulator.toMetrics();
  }

  @Override
  public Coder<StoreMetricsAccumulator> getAccumulatorCoder(
      CoderRegistry registry, Coder<OrderEvent> inputCoder) throws CannotProvideCoderException {
    return SerializableCoder.of(StoreMetricsAccumulator.class);
  }

  @Override
  public Coder<StoreMetrics> getDefaultOutputCoder(
      CoderRegistry registry, Coder<OrderEvent> inputCoder) throws CannotProvideCoderException {
    return SerializableCoder.of(StoreMetrics.class);
  }
}
