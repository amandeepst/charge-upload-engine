package com.worldpay.pms.cue.domain.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class LineCalculationTypeTest {

  @Test
  void testCalcLineType() {
    assertThat(LineCalculationType.PI_MBA.name()).isEqualTo("PI_MBA");
    assertThat(LineCalculationType.PI_RECUR.name()).isEqualTo("PI_RECUR");
  }
}
