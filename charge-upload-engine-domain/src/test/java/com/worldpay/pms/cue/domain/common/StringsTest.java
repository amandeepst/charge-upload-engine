package com.worldpay.pms.cue.domain.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringsTest {

  @Test
  void whenReplaceLastThenLastIsReplaced() {
    String input = "xx yy xx zz";
    assertThat(Strings.replaceLast(input, "xx", "aa")).isEqualTo("xx yy aa zz");
  }

  @Test
  void whenReplaceLastAndRegexDoesNotExistThenNothingIsReplaced() {
    String input = "xx yy xx zz";
    assertThat(Strings.replaceLast(input, "aa", "bb")).isEqualTo("xx yy xx zz");
  }

  @Test
  void whenNullIfEmptyOrWhitespaceAndNullStringThenReturnNull() {
    String input = null;
    assertThat(Strings.nullIfEmptyOrWhitespace(input)).isEqualTo(null);
  }

  @Test
  void whenNullIfEmptyOrWhitespaceAndEmptyStringThenReturnNull() {
    String input = "";
    assertThat(Strings.nullIfEmptyOrWhitespace(input)).isEqualTo(null);
  }

  @Test
  void whenNullIfEmptyOrWhitespaceAndWhitespaceStringThenReturnNull() {
    String input = " ";
    assertThat(Strings.nullIfEmptyOrWhitespace(input)).isEqualTo(null);
  }

  @Test
  void whenNullIfEmptyOrWhitespaceAndNonEmptyNonWhitespaceStringThenReturnString() {
    String input = "xyz";
    assertThat(Strings.nullIfEmptyOrWhitespace(input)).isEqualTo(input);
  }
}
