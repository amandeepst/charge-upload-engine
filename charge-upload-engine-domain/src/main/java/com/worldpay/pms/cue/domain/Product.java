package com.worldpay.pms.cue.domain;

import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;

@Value
public class Product implements Serializable {

  public static final Product NONE = fromCode("NONE");

  @NonNull String productCode;
  String productCategory;

  public static Product fromCode(String code, String productCategory) {
    return new Product(code, productCategory);
  }

  public static Product fromCode(String code) {
    return new Product(code, "NONE");
  }
}
