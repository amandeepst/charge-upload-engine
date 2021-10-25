package com.worldpay.pms.cue.domain.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class AccountTypeTest {

  @Test
  void failsWhenUnknownAccountTypeCode() {
    assertThrows(InvalidConfigurationException.class, () -> AccountType.from("UNKNOWN"));
  }

  @Test
  void canParseAllKnownCodes() {
    assertThat(AccountType.from("CHRG")).isEqualTo(AccountType.CHARGING);
    assertThat(AccountType.from("chrg")).isEqualTo(AccountType.CHARGING);
    assertThat(AccountType.from("FUND")).isEqualTo(AccountType.FUNDING);
    assertThat(AccountType.from("fund")).isEqualTo(AccountType.FUNDING);
    assertThat(AccountType.from("CHBK")).isEqualTo(AccountType.CHARGEBACK);
    assertThat(AccountType.from("chbk")).isEqualTo(AccountType.CHARGEBACK);
    assertThat(AccountType.from("CRWD")).isEqualTo(AccountType.CARD_REWARD);
    assertThat(AccountType.from("crwd")).isEqualTo(AccountType.CARD_REWARD);
  }
}
