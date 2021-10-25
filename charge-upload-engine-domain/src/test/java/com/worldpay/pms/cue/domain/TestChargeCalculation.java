package com.worldpay.pms.cue.domain;

import static com.worldpay.pms.cue.domain.samples.Transactions.CHRG_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.RECR_PENDING_CHARGE;
import static com.worldpay.pms.cue.domain.samples.Transactions.VALID_RECURRING_RESULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class TestChargeCalculation {

  @Test
  void testChargeCalculationForNonRecr() {
    BigDecimal amount = CHRG_PENDING_CHARGE.calculateCharge();
    assertThat(amount).isEqualTo(BigDecimal.valueOf(30));
  }

  @Test
  void testChargeCalculationRecr() {
    BigDecimal amount = RECR_PENDING_CHARGE.calculateCharge();
    assertThat(amount).isEqualTo(BigDecimal.valueOf(20));
  }

  @Test
  void testChargeCalculationForRecrResult() {
    BigDecimal amount = VALID_RECURRING_RESULT.calculateCharge();
    assertThat(amount).isEqualTo(BigDecimal.valueOf(2));
  }

  @Test
  void check() {
    String str = " ";
    System.out.println(str.isEmpty());
  }
}
