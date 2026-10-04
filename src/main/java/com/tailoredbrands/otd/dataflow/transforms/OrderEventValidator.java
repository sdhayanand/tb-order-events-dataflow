package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.EventTypes;
import com.tailoredbrands.otd.dataflow.model.Order;
import com.tailoredbrands.otd.dataflow.model.OrderEvent;
import com.tailoredbrands.otd.dataflow.model.OrderLine;
import com.tailoredbrands.otd.dataflow.model.OrderTypes;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;

/** Validates an {@link OrderEvent} against ARCHITECTURE.md section 3.1. */
public final class OrderEventValidator implements Validator<OrderEvent> {

  private static final long serialVersionUID = 1L;

  @Override
  public List<String> validate(OrderEvent e) {
    List<String> problems = new ArrayList<>();
    if (Validator.isBlank(e.eventId())) {
      problems.add("eventId is required");
    }
    if (Validator.isBlank(e.eventType())) {
      problems.add("eventType is required");
    } else if (!EventTypes.ORDER_EVENT_TYPES.contains(e.eventType())) {
      problems.add("eventType '" + e.eventType() + "' not in " + EventTypes.ORDER_EVENT_TYPES);
    }
    if (!Times.isRfc3339(e.eventTime())) {
      problems.add("eventTime must be an RFC-3339 timestamp, got '" + e.eventTime() + "'");
    }
    if (Validator.isBlank(e.schemaVersion())) {
      problems.add("schemaVersion is required");
    }
    if (Validator.isBlank(e.source())) {
      problems.add("source is required");
    } else if (!EventTypes.ORDER_SOURCES.contains(e.source())) {
      problems.add("source '" + e.source() + "' not in " + EventTypes.ORDER_SOURCES);
    }
    Order o = e.order();
    if (o == null) {
      problems.add("order is required");
      return problems;
    }
    if (Validator.isBlank(o.orderId())) {
      problems.add("order.orderId is required");
    }
    if (Validator.isBlank(o.orderType())) {
      problems.add("order.orderType is required");
    } else if (!OrderTypes.ALL.contains(o.orderType())) {
      problems.add("order.orderType '" + o.orderType() + "' not in " + OrderTypes.ALL);
    }
    if (Validator.isBlank(o.channel())) {
      problems.add("order.channel is required");
    }
    if (Validator.isBlank(o.storeId())) {
      problems.add("order.storeId is required");
    }
    if (Validator.isBlank(o.currency())) {
      problems.add("order.currency is required");
    }
    if (o.totalAmount() == null) {
      problems.add("order.totalAmount is required");
    } else if (o.totalAmount() < 0) {
      problems.add("order.totalAmount must be >= 0, got " + o.totalAmount());
    }
    if (o.orderedAt() != null && !Times.isRfc3339(o.orderedAt())) {
      problems.add("order.orderedAt must be an RFC-3339 timestamp");
    }
    if (o.promisedDate() != null && !Times.isIsoDate(o.promisedDate())) {
      problems.add("order.promisedDate must be an ISO date (yyyy-MM-dd)");
    }
    List<OrderLine> lines = o.safeLines();
    if (EventTypes.ORDER_CREATED.equals(e.eventType()) && lines.isEmpty()) {
      problems.add("order.lines must not be empty for ORDER_CREATED");
    }
    for (int i = 0; i < lines.size(); i++) {
      OrderLine l = lines.get(i);
      String p = "order.lines[" + i + "].";
      if (l == null) {
        problems.add(p + " is null");
        continue;
      }
      if (l.lineNumber() == null || l.lineNumber() < 1) {
        problems.add(p + "lineNumber must be >= 1");
      }
      if (Validator.isBlank(l.sku())) {
        problems.add(p + "sku is required");
      }
      if (l.quantity() == null || l.quantity() < 1) {
        problems.add(p + "quantity must be >= 1");
      }
      if (l.unitPrice() == null || l.unitPrice() < 0) {
        problems.add(p + "unitPrice must be >= 0");
      }
      if (Validator.isBlank(l.fulfillmentType())) {
        problems.add(p + "fulfillmentType is required");
      }
      if (l.needsAlteration() && l.alteration() == null) {
        problems.add(p + "alteration block is required when fulfillmentType == ALTERATION");
      }
    }
    return problems;
  }
}
