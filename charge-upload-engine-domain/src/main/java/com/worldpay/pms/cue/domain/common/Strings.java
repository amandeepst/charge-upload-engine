package com.worldpay.pms.cue.domain.common;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Strings {

  public static boolean isNotNullOrEmptyOrWhitespace(String s) {
    return s != null && !s.trim().isEmpty();
  }

  public static String nullIfEmptyOrWhitespace(String s) {
    return isNotNullOrEmptyOrWhitespace(s) ? s : null;
  }

  public static String replaceLast(String text, String regex, String replacement) {
    return text.replaceFirst("(?s)(.*)" + regex, "$1" + replacement);
  }
}
