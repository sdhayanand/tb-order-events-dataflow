package com.tailoredbrands.otd.dataflow.transforms;

import java.io.Serializable;
import java.util.List;

/**
 * JSON-schema-like structural validation of a parsed event. Returns a (possibly empty) list of
 * human-readable problems; an empty list means "valid". Implementations must be stateless so a
 * single instance can be serialized into a DoFn.
 */
public interface Validator<T> extends Serializable {

  List<String> validate(T event);

  static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }
}
