package com.tailoredbrands.otd.dataflow.model;

/**
 * Line fulfillment types. The contract only pins {@code STORE_PICKUP} and {@code ALTERATION} in its
 * examples, so validation requires the field to be present but does not reject unknown values.
 */
public final class FulfillmentTypes {

  public static final String STORE_PICKUP = "STORE_PICKUP";
  public static final String ALTERATION = "ALTERATION";
  public static final String SHIP_TO_HOME = "SHIP_TO_HOME";
  public static final String SHIP_TO_STORE = "SHIP_TO_STORE";
  public static final String VENDOR_DIRECT = "VENDOR_DIRECT";
  public static final String RENTAL = "RENTAL";

  private FulfillmentTypes() {}
}
