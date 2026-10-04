package com.tailoredbrands.otd.dataflow.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Single, shared Jackson mapper configured per CONVENTIONS.md: JavaTimeModule, ISO timestamps,
 * unknown properties ignored, nulls omitted on output. The mapper is thread-safe and lives in a
 * static field, so it is created once per worker JVM and never serialized with a DoFn.
 */
public final class Json {

  private static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .addModule(new JavaTimeModule())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
          .serializationInclusion(JsonInclude.Include.NON_NULL)
          .build();

  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<LinkedHashMap<String, Object>>() {};

  private Json() {}

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  public static <T> T read(String json, Class<T> type) throws JsonProcessingException {
    return MAPPER.readValue(json, type);
  }

  public static <T> T read(byte[] json, Class<T> type) throws java.io.IOException {
    return MAPPER.readValue(json, type);
  }

  public static LinkedHashMap<String, Object> readMap(String json) throws JsonProcessingException {
    return MAPPER.readValue(json, MAP_TYPE);
  }

  /** Serializes any object; throws an unchecked exception because our model is always writable. */
  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize " + value.getClass().getName(), e);
    }
  }

  /** Writes a BigQuery {@code TableRow} (or any Map) as one compact JSON line. */
  public static String writeRow(Map<String, Object> row) {
    return write(toPlain(row));
  }

  /**
   * Recursively converts {@code TableRow}/{@code GenericData} maps into plain {@link LinkedHashMap}s
   * and {@link List}s so Jackson sees ordinary collections.
   */
  @SuppressWarnings("unchecked")
  public static Object toPlain(Object value) {
    if (value instanceof Map<?, ?> map) {
      LinkedHashMap<String, Object> out = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : map.entrySet()) {
        out.put(String.valueOf(e.getKey()), toPlain(e.getValue()));
      }
      return out;
    }
    if (value instanceof List<?> list) {
      return list.stream().map(Json::toPlain).toList();
    }
    return value;
  }
}
