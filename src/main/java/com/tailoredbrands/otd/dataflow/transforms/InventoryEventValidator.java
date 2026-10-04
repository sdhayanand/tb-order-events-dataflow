package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.EventTypes;
import com.tailoredbrands.otd.dataflow.model.InventoryEvent;
import com.tailoredbrands.otd.dataflow.model.InventoryLine;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;

/** Validates an {@link InventoryEvent} against ARCHITECTURE.md section 3.2. */
public final class InventoryEventValidator implements Validator<InventoryEvent> {

  private static final long serialVersionUID = 1L;

  @Override
  public List<String> validate(InventoryEvent e) {
    List<String> problems = new ArrayList<>();
    if (Validator.isBlank(e.eventId())) {
      problems.add("eventId is required");
    }
    if (Validator.isBlank(e.eventType())) {
      problems.add("eventType is required");
    } else if (!EventTypes.INVENTORY_EVENT_TYPES.contains(e.eventType())) {
      problems.add("eventType '" + e.eventType() + "' not in " + EventTypes.INVENTORY_EVENT_TYPES);
    }
    if (!Times.isRfc3339(e.eventTime())) {
      problems.add("eventTime must be an RFC-3339 timestamp, got '" + e.eventTime() + "'");
    }
    if (Validator.isBlank(e.orderId())) {
      problems.add("orderId is required");
    }
    if (Validator.isBlank(e.storeId())) {
      problems.add("storeId is required");
    }
    List<InventoryLine> lines = e.safeLines();
    if (lines.isEmpty()) {
      problems.add("lines must not be empty");
    }
    for (int i = 0; i < lines.size(); i++) {
      InventoryLine l = lines.get(i);
      String p = "lines[" + i + "].";
      if (l == null) {
        problems.add(p + " is null");
        continue;
      }
      if (Validator.isBlank(l.sku())) {
        problems.add(p + "sku is required");
      }
      if (l.quantity() == null || l.quantity() < 1) {
        problems.add(p + "quantity must be >= 1");
      }
      if (Validator.isBlank(l.status())) {
        problems.add(p + "status is required");
      } else if (!EventTypes.INVENTORY_LINE_STATUSES.contains(l.status())) {
        problems.add(p + "status '" + l.status() + "' not in " + EventTypes.INVENTORY_LINE_STATUSES);
      }
    }
    return problems;
  }
}
