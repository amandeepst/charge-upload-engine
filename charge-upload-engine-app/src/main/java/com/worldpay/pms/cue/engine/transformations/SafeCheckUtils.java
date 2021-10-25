package com.worldpay.pms.cue.engine.transformations;

import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.spark.core.PMSException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SafeCheckUtils {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class RecurringIdentifierWrapper {

    private RecurringIdentifierKey key;
    private RecurringIdentifierRow row;

    public static RecurringIdentifierWrapper fromRecurringIdentifier(RecurringIdentifierRow row) {
      return new RecurringIdentifierWrapper(
          new RecurringIdentifierKey(row.getFrequencyIdentifier(), row.getSourceId()), row);
    }

    public static RecurringIdentifierWrapper reduceRecurringIdentifierRow(
        RecurringIdentifierWrapper x, RecurringIdentifierWrapper y) {

      if (x.getRow().getSourceId() == null || y.getRow().getSourceId() == null) {
        throw new PMSException("Can't reduce RecurringIdentifier with Null SourceId");
      }

      if (!x.getRow().getSourceId().equals(y.getRow().getSourceId())) {
        throw new PMSException(
            "Can't reduce RecurringIdentifier with different keys {} and {}",
            x.getRow().getSourceId(),
            y.getRow().getSourceId());
      }

      return x.getRow().getCutoffDate().after(y.getRow().getCutoffDate()) ? x : y;
    }
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecurringIdentifierKey {

    String frequencyIdentifier;
    String sourceId;
  }
}
