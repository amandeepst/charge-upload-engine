package com.worldpay.pms.cue.domain.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.pce.common.DomainError;
import org.junit.jupiter.api.Test;

public class ErrorCatalogTest {

  @Test
  void testInvalidCurrency() {
    DomainError dE = ErrorCatalog.invalidCurrency(null);
    assertThat("invalid currency 'null'").isEqualTo(dE.getMessage());
  }

  @Test
  void testInvalidSubAccountType() {
    DomainError dE = ErrorCatalog.invalidSubAccountType("abc");
    assertThat("invalid subAccountType 'abc'").isEqualTo(dE.getMessage());
  }

  @Test
  void testInvalidProductIdentifier() {
    DomainError dE = ErrorCatalog.invalidProductIdentifier("");
    assertThat("invalid productIdentifier ''").isEqualTo(dE.getMessage());
  }

  @Test
  void testInvalidFrequencyIdentifier() {
    DomainError dE = ErrorCatalog.invalidFrequencyIdentifier("  ");
    assertThat("invalid frequencyIdentifier '  '").isEqualTo(dE.getMessage());
  }

  @Test
  void testIfFieldIsNull() {
    DomainError dE = ErrorCatalog.unexpectedNull("test_field");
    assertThat("Unexpected null field `TEST_FIELD`").isEqualTo(dE.getMessage());
  }
}
