package com.tailoredbrands.otd.dataflow.model;

import java.util.Set;

/** Enumerations from the canonical event contract (ARCHITECTURE.md section 3). */
public final class EventTypes {

  public static final String ORDER_CREATED = "ORDER_CREATED";
  public static final String ORDER_UPDATED = "ORDER_UPDATED";
  public static final String ORDER_CANCELLED = "ORDER_CANCELLED";
  public static final Set<String> ORDER_EVENT_TYPES =
      Set.of(ORDER_CREATED, ORDER_UPDATED, ORDER_CANCELLED);

  public static final Set<String> ORDER_SOURCES =
      Set.of("ORDER_INTAKE_API", "LEGACY_SOAP_ADAPTER", "TIBCO_EMS_BRIDGE", "REPLAY");

  public static final Set<String> INVENTORY_EVENT_TYPES =
      Set.of("INVENTORY_RESERVED", "INVENTORY_BACKORDERED", "INVENTORY_RELEASED");

  public static final Set<String> INVENTORY_LINE_STATUSES =
      Set.of("RESERVED", "BACKORDERED", "RELEASED");

  public static final Set<String> SHIPMENT_EVENT_TYPES =
      Set.of("SHIPMENT_CREATED", "SHIPMENT_UPDATED");

  public static final Set<String> SHIPMENT_STATUSES =
      Set.of("LABEL_CREATED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION");

  private EventTypes() {}
}
