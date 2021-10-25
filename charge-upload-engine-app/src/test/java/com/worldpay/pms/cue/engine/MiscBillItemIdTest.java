package com.worldpay.pms.cue.engine;

import static com.worldpay.pms.cue.engine.mbi.MiscBillableItem.generateBillItemId;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class MiscBillItemIdTest {

  @Test
  void testPessimisticGeneration() {
    // an ID assigned after 6 years of running 3 times a day, on the 100K'th element of
    // partition 1000
    String id = generateBillItemId(6 * 365 * 3, 1000, 100000);
    log.info("Generated id:`{}`", id);
    assertThat(id).startsWith("U");
    assertThat(id).hasSize(12);
  }
}
