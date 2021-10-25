package com.worldpay.pms.mdu.engine.transformations;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.ACCOUNT_HIERARCHY_ENCODER;
import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.ACCOUNT_HIERARCHY_DATA_ROW_ENCODER;
import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.AccountHierarchyCancellationTransformation.mapCurrentHierarchyAndCancelUpdatesToUpdateResult;
import static com.worldpay.pms.testing.junit.SparkContextHeavyUsage.datasetOf;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import io.vavr.collection.List;
import lombok.val;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class AccountHierarchyCancellationTransformationTest {

  private static final String[] UPDATE_FLG = {"updated"};

  @Test
  void whenNoActiveHierarchyAndUpdateComes() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(ACCOUNT_HIERARCHY_ENCODER),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG))
            .collectAsList();
    assertError(
        result,
        2,
        "No active account hierarchy ['PO00000001', 'PO00000002'] found for cancellation",
        "No active account hierarchy ['PO00000001', 'PO00000002'] found for cancellation");
  }

  @Test
  void whenNoOverlappingUpdateForActiveHierarchy() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG))
            .collectAsList();
    assertError(
        result,
        1,
        "No active account hierarchy ['PO00000001', 'PO00000002'] found having overlapping interval with ['2021-02-01', '2021-02-28']");
  }

  @Test
  void whenUpdateOverlapsWithMultipleActiveHierarchy() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_JAN_TO_FEB_CANCEL_FLG))
            .collectAsList();

    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED_WITH_JAN_TO_FEB_RANGE,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED_WITH_JAN_TO_FEB_RANGE);
  }

  @Test
  void whenPartialUpdateOverlapsWithMultipleActiveHierarchy() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG))
            .collectAsList();

    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED);
  }

  @Test
  void whenMultipleOverlappingUpdatesForMultipleActiveHierarchy() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_WITH_FEB_VALIDITY_AND_CANCEL_FLG))
            .collectAsList();

    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_JAN_CANCELLED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_FEB_CANCELLED);
  }

  @Test
  void whenUnhandledErrorWithDomainServiceThenTaskDoesNotFailAndUpdateIsMarkedAsFailed() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_CANCEL_FLG_AND_TXN_HEADER_ID_NULL))
            .collectAsList();

    assertSuccess(
        result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
  }

  @Test
  void
      whenUnhandledErrorWhileMappingUpdatesTOUpdateResultThenTaskDoesNotFailAndAllUpdatesAreMarkedAsFailed() {
    val result =
        mapCurrentHierarchyAndCancelUpdatesToUpdateResult(
                datasetOf(
                    ACCOUNT_HIERARCHY_ENCODER,
                    ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_CANCEL_FLG_AND_VALID_FROM_NULL,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG))
            .collectAsList();

    assertThat(getErrors(result)).hasSize(2);
    assertSuccess(
        result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
  }

  private void assertSuccess(
      java.util.List<AccountHierarchyUpdateResult> result,
      int count,
      AccountHierarchy... hierarchies) {
    List<AccountHierarchy> success =
        List.ofAll(result)
            .filter(AccountHierarchyUpdateResult::isSuccess)
            .flatMap(updateResult -> List.of(updateResult.getSuccess()));

    assertThat(success).hasSize(count);
    assertThat(success)
        .usingElementComparatorIgnoringFields(UPDATE_FLG)
        .containsExactlyInAnyOrder(hierarchies);
  }

  private void assertError(
      java.util.List<AccountHierarchyUpdateResult> result, int count, String... message) {
    List<ErrorAccountHierarchy> errors = getErrors(result);
    assertThat(errors).hasSize(count);
    assertThat(errors.map(ErrorAccountHierarchy::getMessage).mkString("\n"))
        .isEqualTo(List.of(message).mkString("\n"));
  }

  private List<ErrorAccountHierarchy> getErrors(
      java.util.List<AccountHierarchyUpdateResult> result) {
    return List.ofAll(result)
        .filter(AccountHierarchyUpdateResult::isError)
        .flatMap(updateResult -> List.of(updateResult.getErrors()));
  }
}
