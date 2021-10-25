package com.worldpay.pms.cue.domain.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class StatusTest {

  @Test
  void testStatusActiveOrInactive() {
    assertThat(Status.ACTIVE.name()).isEqualTo("ACTIVE");
    assertThat(Status.INACTIVE.name()).isEqualTo("INACTIVE");
  }
}
