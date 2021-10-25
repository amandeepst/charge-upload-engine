package com.worldpay.pms.cue.engine.recurring;

import static com.google.common.base.Strings.nullToEmpty;
import static com.worldpay.pms.cue.domain.common.Status.ACTIVE;
import static com.worldpay.pms.cue.domain.common.Status.INACTIVE;

import com.worldpay.pms.cue.domain.RecurringRow;
import com.worldpay.pms.cue.domain.common.AccountType;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.spark.core.PMSException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@With
public class RecurringChargeRow implements RecurringRow {

  private static final String CAN_FLG = "Y";
  private static final int CURRENT_CENTURY = (LocalDateTime.now().getYear() / 100) * 100;
  private static final int HUNDRED_YEARS = 100;
  @NonNull String recurringChargeIdentifier;
  @NonNull String txnHeaderId;
  @NonNull String productIdentifier;
  @NonNull String legalCounterparty;
  @NonNull String division;
  @NonNull String partyIdentifier;
  @NonNull String subAccount;
  String frequencyIdentifier;
  String currency;
  BigDecimal price;
  @NonNull long quantity;
  @NonNull Timestamp validFrom;
  Timestamp validTo;
  @NonNull String status;
  @NonNull String sourceId;
  String recurringIdentifierForUpdation;

  public static RecurringChargeRow of(PendingBillableChargeRow pendingBillableCharge) {
    // adding 100 years into valid_to, to make it valid during century if default timestamp is
    // mapped via interface
    Timestamp processedValidTo =
        checkIfValidToIsDefaultTimestamp(pendingBillableCharge.getValidTo())
            ? Timestamp.valueOf(LocalDateTime.of(CURRENT_CENTURY + HUNDRED_YEARS, 1, 1, 0, 0, 0))
            : pendingBillableCharge.getValidTo();
    return new RecurringChargeRow(
        UUID.randomUUID().toString(),
        pendingBillableCharge.getTxnHeaderId(),
        pendingBillableCharge.getProductIdentifier(),
        pendingBillableCharge.getLcp(),
        pendingBillableCharge.getDivision(),
        pendingBillableCharge.getPartyId(),
        AccountType.CHARGING.code,
        pendingBillableCharge.getFrequencyIdentifier(),
        pendingBillableCharge.getCurrency(),
        pendingBillableCharge.getRecurringRate(),
        pendingBillableCharge.getQuantity(),
        pendingBillableCharge.getValidFrom(),
        processedValidTo,
        getStatus(
            pendingBillableCharge.getValidFrom(),
            processedValidTo,
            pendingBillableCharge.getCancellationFlag()),
        pendingBillableCharge.getRecurringIdentifier(),
        pendingBillableCharge.getRecurringIdentifierForUpdation());
  }

  private static String getStatus(Timestamp validFrom, Timestamp validTo, String cancellationFlag) {
    if (validFrom.after(validTo)
        || nullToEmpty(cancellationFlag).trim().equalsIgnoreCase(CAN_FLG)) {
      return INACTIVE.name();
    }
    return ACTIVE.name();
  }

  public static RecurringChargeRow reduce(RecurringChargeRow x, RecurringChargeRow y) {
    if (!x.getSourceId().equals(y.getSourceId())) {
      throw new PMSException(
          "Can't reduce RecurringCharge with different keys {} and {}",
          x.getSourceId(),
          y.getSourceId());
    }
    return x.getTxnHeaderId().compareTo(y.getTxnHeaderId()) > 0 ? x : y;
  }

  private static boolean checkIfValidToIsDefaultTimestamp(Timestamp t) {
    String ts = t.toString();
    return ts.contains("00-01-01 00:00:00")
        && Integer.parseInt(ts.substring(0, ts.indexOf('-'))) >= CURRENT_CENTURY;
  }
}
