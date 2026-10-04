package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.EventTypes;
import com.tailoredbrands.otd.dataflow.model.ShipmentEvent;
import com.tailoredbrands.otd.dataflow.util.Times;
import java.util.ArrayList;
import java.util.List;

/** Validates a {@link ShipmentEvent} against ARCHITECTURE.md section 3.3. */
public final class ShipmentEventValidator implements Validator<ShipmentEvent> {

  private static final long serialVersionUID = 1L;

  @Override
  public List<String> validate(ShipmentEvent e) {
    List<String> problems = new ArrayList<>();
    if (Validator.isBlank(e.eventId())) {
      problems.add("eventId is required");
    }
    if (Validator.isBlank(e.eventType())) {
      problems.add("eventType is required");
    } else if (!EventTypes.SHIPMENT_EVENT_TYPES.contains(e.eventType())) {
      problems.add("eventType '" + e.eventType() + "' not in " + EventTypes.SHIPMENT_EVENT_TYPES);
    }
    if (!Times.isRfc3339(e.eventTime())) {
      problems.add("eventTime must be an RFC-3339 timestamp, got '" + e.eventTime() + "'");
    }
    if (Validator.isBlank(e.orderId())) {
      problems.add("orderId is required");
    }
    if (Validator.isBlank(e.trackingNumber())) {
      problems.add("trackingNumber is required");
    }
    if (Validator.isBlank(e.carrier())) {
      problems.add("carrier is required");
    }
    if (Validator.isBlank(e.status())) {
      problems.add("status is required");
    } else if (!EventTypes.SHIPMENT_STATUSES.contains(e.status())) {
      problems.add("status '" + e.status() + "' not in " + EventTypes.SHIPMENT_STATUSES);
    }
    if (e.statusTime() != null && !Times.isRfc3339(e.statusTime())) {
      problems.add("statusTime must be an RFC-3339 timestamp");
    }
    return problems;
  }
}
