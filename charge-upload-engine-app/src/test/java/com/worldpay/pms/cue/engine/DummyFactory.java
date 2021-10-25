package com.worldpay.pms.cue.engine;

import com.worldpay.pms.cue.engine.common.Factory;
import io.vavr.control.Try;
import lombok.Value;

@Value
public class DummyFactory<T> implements Factory<T> {

  T value;

  @Override
  public Try<T> build() {
    return Try.success(value);
  }
}
