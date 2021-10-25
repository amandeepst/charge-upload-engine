package com.worldpay.pms.cue.domain.samples;

import com.worldpay.pms.cue.domain.PendingBillableCharge;
import com.worldpay.pms.cue.domain.RecurringRow;
import com.worldpay.pms.cue.domain.TestPendingBillableCharge;
import com.worldpay.pms.cue.domain.TestPendingBillableCharge.TestPendingBillableChargeBuilder;
import com.worldpay.pms.cue.domain.TestRecurringResult;
import com.worldpay.pms.cue.domain.TestRecurringRow;
import java.math.BigDecimal;
import java.sql.Timestamp;

public class Transactions {

  public static final TestPendingBillableChargeBuilder INVALID_PENDING_CHARGE =
      TestPendingBillableCharge.builder()
          .txnHeaderId("87755442")
          .partyId("PARTY_ID_TEST")
          .lcp("PO1100000001")
          .subAccountType("TEST")
          .paymentNarrative("N")
          .sourceId("87755442")
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"));

  public static final PendingBillableCharge INVALID_NON_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS =
      INVALID_PENDING_CHARGE.build();

  public static final PendingBillableCharge INVALID_RECR_PENDING_CHARGE_ALL_INVALID_FIELDS =
      INVALID_PENDING_CHARGE
          .productIdentifier("ADHOCCH_G")
          .currency("GB_P")
          .frequencyIdentifier("WPD_Y")
          .subAccountType("RECR")
          .build();

  public static final PendingBillableCharge INVALID_PENDING_CHARGE_NO_CURRENCY_AND_PRICE =
      INVALID_PENDING_CHARGE
          .accountId("11111")
          .subAccountId("22222")
          .productIdentifier("ADHOCCHG")
          .subAccountType("CHRG")
          .currency("GB_P")
          .build();

  public static final RecurringRow INVALID_RECURRING_ROW =
      TestRecurringRow.builder()
          .frequencyIdentifier("WPD_Y")
          .productIdentifier("ADHOCCH_G")
          .build();

  public static final PendingBillableCharge CHRG_PENDING_CHARGE_WITH_INVALID_PARTYID_AND_LCP =
      TestPendingBillableCharge.builder()
          .txnHeaderId("8451251")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .paymentNarrative("CHG 12 89 DB")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("CHRG")
          .build();

  public static final PendingBillableCharge RECR_PENDING_CHARGE =
      TestPendingBillableCharge.builder()
          .txnHeaderId("87755442")
          .partyId("REC000000005")
          .lcp("PO1100000001")
          .division("00001")
          .paymentNarrative("N")
          .sourceType("REC_CHG")
          .sourceId("87755442")
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("RECR")
          .price(BigDecimal.TEN)
          .recurringRate(BigDecimal.ONE)
          .quantity(2L)
          .ilmDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .recurringIdentifier("87755442")
          .accountId("11111")
          .subAccountId("22222")
          .build();

  public static final TestRecurringResult INVALID_RECURRING_RESULT =
      TestRecurringResult.builder()
          .recurringChargeId("12345")
          .accountId("1111")
          .currency("GBP")
          .frequencyIdentifier("WPDY")
          .cutoffDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .productIdentifier("XYZ")
          .legalCounterParty("PO1100000001")
          .division("00001")
          .partyId("REC000000005")
          .subAccount("RECR")
          .subAccountId(null)
          .accountId(null)
          .build();

  public static final TestRecurringResult VALID_RECURRING_RESULT =
      TestRecurringResult.builder()
          .recurringChargeId("12345")
          .accountId("1111")
          .currency("GBP")
          .frequencyIdentifier("WPDY")
          .cutoffDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .legalCounterParty("PO1100000001")
          .division("00001")
          .partyId("REC000000005")
          .subAccount("RECR")
          .recurringSourceId("12345")
          .subAccountId("12333")
          .price(BigDecimal.ONE)
          .quantity(2L)
          .build();

  public static final PendingBillableCharge CHRG_PENDING_CHARGE =
      TestPendingBillableCharge.builder()
          .txnHeaderId("8451251")
          .partyId("PER1000009")
          .lcp("PO1100000003")
          .division("00001")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .paymentNarrative("N")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("CHRG")
          .accountId("11111")
          .subAccountId("22222")
          .quantity(3)
          .build();

  public static final PendingBillableCharge NULL_LCP_PENDING_CHARGE =
      TestPendingBillableCharge.builder()
          .txnHeaderId("8451251")
          .partyId("PER1000009")
          .lcp(null)
          .division("00001")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .paymentNarrative("N")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("CHRG")
          .accountId("11111")
          .subAccountId("22222")
          .quantity(3)
          .build();

  public static final PendingBillableCharge INVALID_CHRG_PENDING_CHARGE =
      TestPendingBillableCharge.builder()
          .txnHeaderId("8451251")
          .partyId("PER1000009")
          .lcp("PO1100000003")
          .division("00001")
          .subAccountType("CHRG")
          .price(BigDecimal.TEN)
          .paymentNarrative("N")
          .validFrom(Timestamp.valueOf("2020-01-05 00:00:00"))
          .productIdentifier("ADHOCCHG")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .subAccountType("CHRG")
          .accountId(null)
          .subAccountId(null)
          .quantity(3)
          .build();

  public static final TestRecurringRow RECR_ROW =
      TestRecurringRow.builder()
          .recurringChargeIdentifier("87755442")
          .productIdentifier("ADHOCCHG")
          .legalCounterparty("PO1100000001")
          .division("00001")
          .partyIdentifier("REC000000005")
          .frequencyIdentifier("WPDY")
          .currency("GBP")
          .price(BigDecimal.valueOf(10.22D))
          .quantity(2L)
          .validFrom(Timestamp.valueOf("2020-01-01 00:00:00"))
          .validTo(Timestamp.valueOf("2020-12-31 00:00:00"))
          .status("ACTIVE")
          .sourceId("87755442")
          .build();

  public static final PendingBillableCharge INVALID_RECURRING_CHARGE_ROW =
      TestPendingBillableCharge.builder()
          .currency(null)
          .recurringIdentifier(" ")
          .productIdentifier(null)
          .subAccountType(null)
          .frequencyIdentifier(null)
          .price(null)
          .recurringRate(null)
          .quantity(1)
          .validTo(null)
          .build();

  public static final PendingBillableCharge INVALID_RECURRING_CHARGE_ROW_NULL_SOURCE_ID =
      TestPendingBillableCharge.builder()
          .currency(null)
          .recurringIdentifier(null)
          .productIdentifier(null)
          .subAccountType(null)
          .frequencyIdentifier(null)
          .price(null)
          .recurringRate(null)
          .quantity(1)
          .validTo(null)
          .build();

  public static final PendingBillableCharge VALID_RECURRING_CHARGE_ROW =
      TestPendingBillableCharge.builder()
          .currency("GBP")
          .recurringIdentifier("rec-2")
          .productIdentifier("ADHOCCHG")
          .subAccountType("RECR")
          .lcp("P0110101")
          .frequencyIdentifier("WPDY")
          .price(BigDecimal.ONE)
          .quantity(1)
          .recurringRate(BigDecimal.ONE)
          .validTo(Timestamp.valueOf("2020-01-01 00:00:00"))
          .build();

  public static final TestRecurringResult INVALID_RECURRING_RESULT_WITH_NULL_FIELDS =
      TestRecurringResult.builder()
          .recurringChargeId("12345")
          .accountId("1111")
          .currency(null)
          .txnHeaderId(null)
          .frequencyIdentifier(null)
          .cutoffDate(Timestamp.valueOf("2020-01-01 00:00:00"))
          .productIdentifier(null)
          .legalCounterParty(null)
          .division(null)
          .partyId(null)
          .subAccount(null)
          .subAccountId("12333")
          .build();
}
