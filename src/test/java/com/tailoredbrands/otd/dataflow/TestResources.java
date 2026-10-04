package com.tailoredbrands.otd.dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Classpath fixture loader shared by the tests. */
public final class TestResources {

  private TestResources() {}

  public static byte[] bytes(String path) {
    try (InputStream in = TestResources.class.getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalArgumentException("Missing test resource " + path);
      }
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String text(String path) {
    return new String(bytes(path), StandardCharsets.UTF_8);
  }
}
