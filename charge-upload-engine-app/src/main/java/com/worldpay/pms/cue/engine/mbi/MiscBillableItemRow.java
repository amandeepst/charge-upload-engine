package com.worldpay.pms.cue.engine.mbi;

import static com.google.common.base.Strings.nullToEmpty;
import static com.worldpay.pms.cue.domain.common.Status.ACTIVE;

import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MiscBillableItemRow {

  private static final String Y_FLG = "Y";

  @NonNull String txnHeaderId;
  @NonNull String partyId;
  @NonNull String legalCounterparty;
  @NonNull String division;
  @NonNull String subAccount;
  @NonNull String accountId;
  @NonNull String subAccountId;
  @NonNull String currency;
  @NonNull String status;
  @NonNull String productIdentifier;
  String paymentNarrative;
  String sourceType;
  String sourceId;
  String eventId;
  long quantity;
  String adhocBillIndicator;
  String fastestSettlementIndicator;
  String individualPaymentIndicator;
  String releaseReserverIndicator;
  String releaseWafIndicator;
  String frequencyIdentifier;
  String caseIdentifier;
  Timestamp debtDate;
  Timestamp cutOffDate;
  Timestamp accruedDate;
  String caseFlag;
  String hashString;

  public static MiscBillableItemRow build(
      PendingBillableChargeRow pendingBillableCharge, Date logicalDate) {
    String caseFlag =
        getCaseFlag(
            pendingBillableCharge.getCaseIdentifier(),
            pendingBillableCharge.getReleaseReserverIndicator(),
            pendingBillableCharge.getReleaseWafIndicator());
    String eventId = pendingBillableCharge.getEventId();
    String sourceType = getSourceType(eventId, pendingBillableCharge.getSourceType());
    String sourceId = getSourceId(eventId, pendingBillableCharge.getSourceId());

    return new MiscBillableItemRow(
        pendingBillableCharge.getTxnHeaderId(),
        pendingBillableCharge.getPartyId(),
        pendingBillableCharge.getLcp(),
        pendingBillableCharge.getDivision(),
        pendingBillableCharge.getSubAccountType(),
        pendingBillableCharge.getAccountId(),
        pendingBillableCharge.getSubAccountId(),
        pendingBillableCharge.getCurrency(),
        ACTIVE.name(),
        pendingBillableCharge.getProductIdentifier(),
        pendingBillableCharge.getPaymentNarrative(),
        sourceType,
        sourceId,
        pendingBillableCharge.getEventId(),
        pendingBillableCharge.getQuantity(),
        pendingBillableCharge.getAdhocBillIndicator(),
        pendingBillableCharge.getFastestSettlementIndicator(),
        pendingBillableCharge.getIndividualPaymentIndicator(),
        pendingBillableCharge.getReleaseReserverIndicator(),
        pendingBillableCharge.getReleaseWafIndicator(),
        null,
        pendingBillableCharge.getCaseIdentifier(),
        pendingBillableCharge.getDebtDate(),
        null,
        new Timestamp(logicalDate.getTime()),
        caseFlag,
        getHashString(
            Stream.of(
                pendingBillableCharge.getAdhocBillIndicator(),
                pendingBillableCharge.getPaymentNarrative(),
                pendingBillableCharge.getReleaseReserverIndicator(),
                pendingBillableCharge.getReleaseWafIndicator(),
                pendingBillableCharge.getFastestSettlementIndicator(),
                caseFlag)));
  }

  public static MiscBillableItemRow build(RecurringResultRow recurringRow) {
    return new MiscBillableItemRow(
        recurringRow.getTxnHeaderId(),
        recurringRow.getPartyId(),
        recurringRow.getLegalCounterParty(),
        recurringRow.getDivision(),
        recurringRow.getSubAccount(),
        recurringRow.getAccountId(),
        recurringRow.getSubAccountId(),
        recurringRow.getCurrency(),
        recurringRow.getStatus(),
        recurringRow.getProductIdentifier(),
        "N",
        "REC_CHG",
        recurringRow.getRecurringSourceId(),
        null,
        recurringRow.getQuantity(),
        "N",
        "N",
        "N",
        "N",
        "N",
        recurringRow.getFrequencyIdentifier(),
        null,
        null,
        recurringRow.getCutoffDate(),
        recurringRow.getCutoffDate(),
        // case_flg for recurring will always be N as releaseReserve and releaseWaf are always N
        "N",
        // for recr, granularity_hash in cm_bchg_attributes_map will always be 0, which we handle in
        // publish script
        // thus setting static value here
        "###");
  }

  static String getCaseFlag(
      String caseIdentifier, String releaseReserverIndicator, String releaseWafIndicator) {
    caseIdentifier = caseIdentifier == null ? "N" : caseIdentifier;
    if (nullToEmpty(releaseReserverIndicator).trim().equalsIgnoreCase(Y_FLG)) {
      return caseIdentifier;
    } else if (nullToEmpty(releaseWafIndicator).trim().equalsIgnoreCase(Y_FLG)) {
      return caseIdentifier;
    } else {
      return "N";
    }
  }

  static String getSourceType(String eventId, String sourceType) {
    return (eventId == null ? sourceType : "EVENT");
  }

  static String getSourceId(String eventId, String sourceId) {
    return (eventId == null ? sourceId : eventId);
  }

  static String getHashString(Stream<String> args) {
    return args.filter(Objects::nonNull).collect(Collectors.joining());
  }
}
