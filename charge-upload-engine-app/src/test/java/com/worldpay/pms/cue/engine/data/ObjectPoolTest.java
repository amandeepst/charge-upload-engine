package com.worldpay.pms.cue.engine.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ObjectPoolTest {

  @Test
  void checkObjectPoolCannotBeInstantiated() {
    assertThatThrownBy(ObjectPool::new, "Should throw error")
        .hasMessage("ObjectPool cannot be instantiated!");
  }

  @Test
  void checkInternMethod() {
    String test = "test";
    assertThat(ObjectPool.intern(test)).isEqualTo(test);
    assertThat(ObjectPool.intern(null)).isNull();
  }
}
