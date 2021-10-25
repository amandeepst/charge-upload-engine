package com.worldpay.pms.cue.engine.common;

import io.vavr.control.Try;
import java.io.Serializable;

public interface Factory<T> extends Serializable {
  /**
   * Creates an instance of type T or returns a failure
   *
   * @return
   */
  Try<T> build();
}
