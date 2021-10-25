package com.worldpay.pms.cue.engine.recurring;

import com.worldpay.pms.cue.engine.transformations.RecurringCharge;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@With
public class RecurringIdentifierRow {

  String sourceId;
  Timestamp cutoffDate;
  String frequencyIdentifier;
  long partitionId;

  public static RecurringIdentifierRow of(RecurringCharge recurringCharge) {
    return new RecurringIdentifierRow.RecurringIdentifierRowBuilder()
        .cutoffDate(recurringCharge.getRecurringResultRow().getCutoffDate())
        .frequencyIdentifier(recurringCharge.getRecurringResultRow().getFrequencyIdentifier())
        .sourceId(recurringCharge.getRecurringResultRow().getRecurringSourceId())
        .build();
  }

  public static RecurringIdentifierRow of(RecurringErrorTransaction errorTransaction) {
    return new RecurringIdentifierRow.RecurringIdentifierRowBuilder()
        .cutoffDate(errorTransaction.getCutoffDate())
        .frequencyIdentifier(errorTransaction.getFrequencyIdentifier())
        .sourceId(errorTransaction.getRecurringSourceId())
        .build();
  }
}
