package com.worldpay.pms.cue.engine.data;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public final class ObjectPool {

  private static final Interner<String> stringPool = Interners.newWeakInterner();

  ObjectPool() {
    throw new AssertionError("ObjectPool cannot be instantiated!");
  }

  public static String intern(String value) {
    if (value == null) {
      return null;
    }

    return stringPool.intern(value);
  }
}
