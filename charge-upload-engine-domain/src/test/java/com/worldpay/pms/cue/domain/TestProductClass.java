package com.worldpay.pms.cue.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestProductClass {

  @Test
  public void testFromCode() {
    Product product = Product.NONE;
    String productCode = product.getProductCode();
    String productCategory = product.getProductCategory();
    assertThat(productCode).isEqualTo("NONE");
    assertThat(productCategory).isEqualTo("NONE");
  }

  @Test
  public void testFromCode1() {
    Product product = Product.fromCode("MREC7120", "MISCELLANEOUS");
    String productCode = product.getProductCode();
    String productCategory = product.getProductCategory();
    assertThat(productCode).isEqualTo("MREC7120");
    assertThat(productCategory).isEqualTo("MISCELLANEOUS");
  }
}
