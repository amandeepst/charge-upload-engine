package com.worldpay.pms.cue.engine.transformations;

import static com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierWrapper.reduceRecurringIdentifierRow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierKey;
import com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierWrapper;
import com.worldpay.pms.spark.core.PMSException;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class SafeCheckUtilsTest {

  public static final RecurringIdentifierWrapper recurringIdentifierWrapper_1 =
      RecurringIdentifierWrapper.builder()
          .key(new RecurringIdentifierKey("WPDY", "12345"))
          .row(new RecurringIdentifierRow("12345", Timestamp.from(Instant.now()), "WPDY", 0))
          .build();

  public static final RecurringIdentifierWrapper recurringIdentifierWrapper_1_with_null_sourceId =
      RecurringIdentifierWrapper.builder()
          .key(new RecurringIdentifierKey("WPDY", null))
          .row(new RecurringIdentifierRow(null, Timestamp.from(Instant.now()), "WPDY", 0))
          .build();

  public static final RecurringIdentifierWrapper recurringIdentifierWrapper_2 =
      RecurringIdentifierWrapper.builder()
          .key(new RecurringIdentifierKey("WPMO", "12346"))
          .row(new RecurringIdentifierRow("12346", Timestamp.from(Instant.now()), "WPMO", 0))
          .build();

  public static final RecurringIdentifierWrapper recurringIdentifierWrapper_2_with_null_sourceId =
      RecurringIdentifierWrapper.builder()
          .key(new RecurringIdentifierKey("WPMO", null))
          .row(new RecurringIdentifierRow(null, Timestamp.from(Instant.now()), "WPMO", 0))
          .build();

  @Test
  void testreduceRecurringIdentifierRow() {

    assertThatThrownBy(
            () ->
                reduceRecurringIdentifierRow(
                    recurringIdentifierWrapper_1, recurringIdentifierWrapper_2))
        .isInstanceOf(PMSException.class)
        .hasMessage(
            "Can't reduce RecurringIdentifier with different keys {} and {}", "12345", "12346");
  }

  @Test
  void testreduceRecurringIdentifierRowWithNullSourceId() {

    assertThatThrownBy(
            () ->
                reduceRecurringIdentifierRow(
                    recurringIdentifierWrapper_1_with_null_sourceId,
                    recurringIdentifierWrapper_2_with_null_sourceId))
        .isInstanceOf(PMSException.class)
        .hasMessage("Can't reduce RecurringIdentifier with Null SourceId");

    assertThatThrownBy(
            () ->
                reduceRecurringIdentifierRow(
                    recurringIdentifierWrapper_1, recurringIdentifierWrapper_2_with_null_sourceId))
        .isInstanceOf(PMSException.class)
        .hasMessage("Can't reduce RecurringIdentifier with Null SourceId");

    assertThatThrownBy(
            () ->
                reduceRecurringIdentifierRow(
                    recurringIdentifierWrapper_1_with_null_sourceId, recurringIdentifierWrapper_2))
        .isInstanceOf(PMSException.class)
        .hasMessage("Can't reduce RecurringIdentifier with Null SourceId");
  }
}
