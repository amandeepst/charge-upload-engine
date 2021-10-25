package com.worldpay.pms.cue.engine.batch;

import static com.worldpay.pms.cue.engine.batch.ChargingBatchHistoryRepository.interpolateHints;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ChargingBatchHistoryRepositoryTest {
  private static final String SQL_EXPR = "/*+ :insert-hints */|/*+ :select-hints */";

  @ParameterizedTest
  @CsvSource({
    "a:b,/*+ a */|/*+ b */",
    ":b,/*+  */|/*+ b */",
    "a:,/*+ a */|/*+  */",
    "a,/*+ a */|/*+  */",
    "'',/*+  */|/*+  */",
    ",/*+  */|/*+  */",
  })
  void cantInterpolateHintsForInsertAndSelect(String hints, String expected) {
    assertThat(interpolateHints(SQL_EXPR, hints)).isEqualTo(expected);
  }
}
