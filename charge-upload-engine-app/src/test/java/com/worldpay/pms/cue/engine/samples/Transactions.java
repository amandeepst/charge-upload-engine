package com.worldpay.pms.cue.engine.samples;

import static com.worldpay.pms.cue.domain.common.LineCalculationType.PI_RECUR;
import static com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow.PendingBillableChargeRowBuilder;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.transformations.RecurringCharge;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Transactions {

  public static final PendingBillableChargeRowBuilder INVALID_PENDING_CHARGE =
      PendingBillableChargeRow.builder()
          .txnHeaderId("87755442")
          .partyId("PARTY_ID_TEST")
          .lcp("PO1100000001")
          .division("00001")
          .subAccountType("TEST")
          .paymentNarrative("N")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .sourceId("87755442")
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"));

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_1 =
      INVALID_PENDING_CHARGE.build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_2 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222 ")
          .productIdentifier(" ")
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_3 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .txnHeaderId("DEF")
          .productIdentifier("ADHOCCHG")
          .subAccountType("CHR_G")
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_4 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .txnHeaderId("ABC")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .subAccountType("CHRG")
          .currency("GB_P")
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_5 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .subAccountType("CHRG")
          .currency("GBP")
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_6 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .currency("GB_P")
          .frequencyIdentifier("WPD_Y")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .subAccountType("RECR")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_7 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPD_Y")
          .currency("GBP")
          .subAccountType("RECR")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_8 =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("RECR")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_WITH_NULL_FIELDS =
      PendingBillableChargeRow.builder()
          .txnHeaderId("8451251")
          .partyId("PER1000009")
          .division(null)
          .lcp("PO1100000003")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .currency("GBP")
          .subAccountType("CHRG")
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final RecurringResultRow INVALID_RECR_CHARGE =
      RecurringResultRow.builder()
          .frequencyIdentifier("ABC")
          .currency("ABC")
          .recurringSourceId("12345")
          .build();

  public static final RecurringResultRow INVALID_RECR_CHARGE_WITH_NULL_SOURCE_ID =
      RecurringResultRow.builder()
          .frequencyIdentifier("ABC")
          .currency("ABC")
          .recurringSourceId(null)
          .build();

  public static final RecurringResultRow INVALID_RECR_CHARGE_2 =
      RecurringResultRow.builder()
          .frequencyIdentifier("PBC")
          .currency("ABC")
          .recurringSourceId("12345")
          .build();

  public static final PendingBillableChargeRow INVALID_PENDING_CHARGE_WITHOUT_SUBACCOUNT =
      PendingBillableChargeRow.builder()
          .txnHeaderId("8451251")
          .price(BigDecimal.TEN)
          .paymentNarrative("CHG 12 89 DB")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("CHRG")
          .accountId("11111")
          .subAccountId("  ")
          .build();

  public static final PendingBillableChargeRow CHRG_PENDING_CHARGE =
      PendingBillableChargeRow.builder()
          .txnHeaderId("8451251")
          .partyId("PER1000009")
          .lcp("PO1100000003")
          .division("00001")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .paymentNarrative("CHG 12 89 DB")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .currency("GBP")
          .subAccountType("CHRG")
          .adhocBillIndicator("Y")
          .fastestSettlementIndicator("Y")
          .individualPaymentIndicator("N")
          .releaseWafIndicator("Y")
          .releaseReserverIndicator("Y")
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final PendingBillableChargeRow RECR_PENDING_CHARGE =
      PendingBillableChargeRow.builder()
          .txnHeaderId("87755442")
          .partyId("REC000000005")
          .lcp("PO1100000001")
          .division("00001")
          .paymentNarrative("N")
          .sourceType("REC_CHG")
          .sourceId("87755442")
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .validTo(Timestamp.valueOf("2020-01-02 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("RECR")
          .recurringRate(BigDecimal.ONE)
          .quantity(1L)
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .recurringIdentifier("87755442")
          .cancellationFlag("N")
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final PendingBillableChargeRow
      RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_INVALID_STATUS =
          PendingBillableChargeRow.builder()
              .txnHeaderId("87755442")
              .partyId("REC000000005")
              .lcp("PO1100000001")
              .division("00001")
              .paymentNarrative("N")
              .sourceType("REC_CHG")
              .sourceId("87755442")
              .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
              .validTo(Timestamp.valueOf("2020-01-02 00:00:00"))
              .productIdentifier("ADHOCCHG")
              .frequencyIdentifier("WPDY")
              .currency("GBP")
              .subAccountType("RECR")
              .recurringRate(BigDecimal.ONE)
              .quantity(1L)
              .firstFailureAt(Date.valueOf("2020-01-01"))
              .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
              .recurringIdentifier("87755443")
              .cancellationFlag("y ")
              .accountId("11111")
              .subAccountId("22222")
              .build();

  public static final PendingBillableChargeRow RECR_PENDING_CHARGE_WITH_VALID_DATE_AND_NULL_STATUS =
      PendingBillableChargeRow.builder()
          .txnHeaderId("87755442")
          .partyId("REC000000005")
          .lcp("PO1100000001")
          .division("00001")
          .paymentNarrative("N")
          .sourceType("REC_CHG")
          .sourceId("87755442")
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .validTo(Timestamp.valueOf("2020-01-02 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("RECR")
          .recurringRate(BigDecimal.ONE)
          .quantity(1L)
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .recurringIdentifier("87755444")
          .cancellationFlag(null)
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final PendingBillableChargeRow
      RECR_PENDING_CHARGE_WITH_INVALID_DATE_AND_VALID_STATUS =
          PendingBillableChargeRow.builder()
              .txnHeaderId("87755442")
              .partyId("REC000000005")
              .lcp("PO1100000001")
              .division("00001")
              .paymentNarrative("N")
              .sourceType("REC_CHG")
              .sourceId("87755442")
              .validFrom(Timestamp.valueOf("2020-01-02 00:00:00"))
              .validTo(Timestamp.valueOf("2020-01-01 00:00:00"))
              .productIdentifier("ADHOCCHG")
              .frequencyIdentifier("WPDY")
              .currency("GBP")
              .subAccountType("RECR")
              .recurringRate(BigDecimal.ONE)
              .quantity(1L)
              .firstFailureAt(Date.valueOf("2020-01-01"))
              .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
              .recurringIdentifier("87755445")
              .cancellationFlag("N")
              .accountId("11111")
              .subAccountId("22222")
              .build();

  public static final VwmBcuAccountRow VWM_BCU_ACCOUNT_ROW_1 =
      VwmBcuAccountRow.builder()
          .accountId("12345")
          .subaccountId("45678")
          .currencyCode("EUR")
          .legalcounterparty("00001")
          .partyId("PO4000398255")
          .partitionId(1)
          .subaccountType("CHBK")
          .build();

  public static final VwmBcuAccountRow VWM_BCU_ACCOUNT_ROW_2 =
      VwmBcuAccountRow.builder()
          .accountId("12345")
          .subaccountId("45678")
          .currencyCode("GBP")
          .legalcounterparty("00001")
          .partyId("REC000000005")
          .partitionId(1)
          .subaccountType("RECR")
          .build();

  public static final VwmBcuAccountRow VWM_BCU_ACCOUNT_ROW_3 =
      VwmBcuAccountRow.builder()
          .accountId("12345")
          .subaccountId("45678")
          .currencyCode("GBP")
          .legalcounterparty("00001")
          .partyId("PER1000009")
          .partitionId(1)
          .subaccountType("CHRG")
          .build();

  public static final PendingBillableChargeRow CHBK_PENDING_CHARGE =
      PendingBillableChargeRow.builder()
          .txnHeaderId("302296")
          .partyId("PO4000398255")
          .lcp("PO1100000001")
          .division("00001")
          .price(BigDecimal.TEN)
          .paymentNarrative("MIGCHGBK")
          .validFrom(Timestamp.valueOf("2020-01-02 00:00:00"))
          .productIdentifier("MIGCHBK")
          .frequencyIdentifier("WPDY")
          .currency("EUR")
          .firstFailureAt(Date.valueOf("2020-01-01"))
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .subAccountType("CHBK")
          .adhocBillIndicator("Y")
          .fastestSettlementIndicator("N")
          .individualPaymentIndicator("N")
          .releaseWafIndicator("Y")
          .releaseReserverIndicator("Y")
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final RecurringResultRow RECR_CHARGE =
      RecurringResultRow.builder()
          .txnHeaderId("12345678")
          .productIdentifier("ADHOCCHG")
          .accountId("11111")
          .subAccountId("99999")
          .legalCounterParty("PO1100000001")
          .division("00001")
          .partyId("REC000000005")
          .subAccount("RECR")
          .accountId("11111")
          .subAccountId("99999")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .price(BigDecimal.TEN)
          .quantity(1L)
          .startDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .endDate(Timestamp.valueOf("2020-12-31 00:00:00"))
          .status("ACTIVE")
          .recurringSourceId("87755442")
          .cutoffDate(Timestamp.valueOf("2020-08-31 00:00:00"))
          .build();

  private static final Charge CHARGE = new Charge(PI_RECUR.name(), BigDecimal.TEN);

  public static final RecurringCharge RECURRING_CHARGE =
      RecurringCharge.builder().recurringResultRow(RECR_CHARGE).charge(CHARGE).build();

  public static final RecurringChargeRow RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850747 =
      RecurringChargeRow.builder()
          .recurringChargeIdentifier("eg2mfpbg3f397i3qg1idcui3go")
          .txnHeaderId("12345678")
          .productIdentifier("MRCD0002")
          .legalCounterparty("PO1100000001")
          .division("00001")
          .partyIdentifier("MTB000000047")
          .subAccount("RECR")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .price(BigDecimal.ONE)
          .quantity(1L)
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .validTo(Timestamp.valueOf("2020-12-31 00:00:00"))
          .status("ACTIVE")
          .sourceId("850747")
          .build();

  public static final RecurringChargeRow RECR_TRANSITIONAL_ROW_WITH_SOURCE_ID_850851 =
      RecurringChargeRow.builder()
          .recurringChargeIdentifier("mk6j0ast5audfoek1u3pin1ces")
          .txnHeaderId("12345678")
          .productIdentifier("MRCD0002")
          .legalCounterparty("PO1100000001")
          .division("00001")
          .partyIdentifier("MTB000000047")
          .subAccount("RECR")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .price(BigDecimal.ONE)
          .quantity(1L)
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .validTo(Timestamp.valueOf("2020-12-31 00:00:00"))
          .status("ACTIVE")
          .sourceId("850851")
          .build();
}
