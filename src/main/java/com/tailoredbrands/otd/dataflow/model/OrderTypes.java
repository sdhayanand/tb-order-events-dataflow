package com.tailoredbrands.otd.dataflow.model;

import java.util.Set;

/** Order types from ARCHITECTURE.md section 1. */
public final class OrderTypes {

  public static final String RETAIL = "RETAIL";
  public static final String TAILORED = "TAILORED";
  public static final String CUSTOM = "CUSTOM";
  public static final String RENTAL = "RENTAL";
  public static final String ECOM = "ECOM";

  public static final Set<String> ALL = Set.of(RETAIL, TAILORED, CUSTOM, RENTAL, ECOM);

  private OrderTypes() {}
}
