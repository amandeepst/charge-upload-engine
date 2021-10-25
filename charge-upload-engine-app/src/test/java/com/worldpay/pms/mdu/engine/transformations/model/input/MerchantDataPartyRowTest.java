package com.worldpay.pms.mdu.engine.transformations.model.input;

import static com.worldpay.pms.mdu.engine.transformations.model.input.MerchantDataPartyRow.sortByTxnHeaderId;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import org.junit.jupiter.api.Test;

public class MerchantDataPartyRowTest {
  private static final Date DATE_2021_03_15 = Date.valueOf("2021-03-15");
  private static final Date DATE_2021_03_16 = Date.valueOf("2021-03-16");
  private static final Date DATE_2021_03_17 = Date.valueOf("2021-03-17");
  private static final MerchantDataAccountRow[] EMPTY_MERCHANT_ACCOUNT_ARRAY =
      new MerchantDataAccountRow[0];

  List<MerchantDataPartyRow> rows = new ArrayList();
  MerchantDataPartyBuild merchantDataPartyRow =
      create().partyId("P011111").validFrom(Date.valueOf("2021-01-01")).country("GBR").state("OH");

  @Test
  void sortByTxnHeaderIdTest() {
    merchantDataPartyRow.txnHeaderId("10005").division("00001").validTo(DATE_2021_03_17).add();
    merchantDataPartyRow.txnHeaderId("10003").division("00003").validTo(null).add();
    merchantDataPartyRow.txnHeaderId("10001").division("00001").validTo(DATE_2021_03_15).add();
    merchantDataPartyRow.txnHeaderId("10002").division("00002").validTo(DATE_2021_03_15).add();
    merchantDataPartyRow.txnHeaderId("10004").division("00001").validTo(DATE_2021_03_16).add();

    Collections.sort(rows, sortByTxnHeaderId());

    assertThat(rows)
        .first()
        .extracting(MerchantDataPartyRow::getValidTo)
        .isEqualTo(DATE_2021_03_15);
    assertThat(rows).last().extracting(MerchantDataPartyRow::getValidTo).isEqualTo(DATE_2021_03_17);
  }

  @Builder(
      builderClassName = "MerchantDataPartyBuild",
      builderMethodName = "create",
      buildMethodName = "add")
  private void createMerchantDataPartyRow(
      String partyId,
      String txnHeaderId,
      String division,
      String country,
      String state,
      Date validFrom,
      Date validTo) {
    rows.add(
        MerchantDataPartyRow.builder()
            .partyId(partyId)
            .txnHeaderId(txnHeaderId)
            .division(division)
            .country(country)
            .state(state)
            .validFrom(validFrom)
            .validTo(validTo)
            .accounts(EMPTY_MERCHANT_ACCOUNT_ARRAY)
            .build());
  }
}
