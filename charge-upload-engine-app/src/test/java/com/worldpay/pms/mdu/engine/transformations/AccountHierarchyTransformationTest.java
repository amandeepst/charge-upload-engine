package com.worldpay.pms.mdu.engine.transformations;

import static com.worldpay.pms.mdu.engine.encoder.Encoders.*;
import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.ACCOUNT_HIERARCHY_DATA_ROW_ENCODER;
import static com.worldpay.pms.mdu.engine.samples.Transactions.*;
import static com.worldpay.pms.mdu.engine.transformations.AccountHierarchyTransformation.deriveAndValidateAccountForHierarchyUpdates;
import static com.worldpay.pms.mdu.engine.transformations.AccountHierarchyTransformation.mapCurrentAccountHierarchyAndUpdatesToSuccessOrError;
import static com.worldpay.pms.testing.junit.SparkContextHeavyUsage.datasetOf;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.domain.model.output.AccountHierarchy;
import com.worldpay.pms.mdu.engine.transformations.model.input.AccountHierarchyDataRow;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import io.vavr.collection.List;
import lombok.val;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class AccountHierarchyTransformationTest {

  private static final String[] UPDATE_FLG_AND_CREATION_DATE = {"creationDate", "updated"};

  @Test
  void whenNoAccountFoundOnNewAccountHierarchy() {
    val result =
        deriveAndValidateAccountForHierarchyUpdates(
                datasetOf(PARTY_ENCODER),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002))
            .collectAsList();

    assertErrorUpdates(
        result,
        1,
        "account not found either for parentPartyId 'PO00000001' childPartyId 'PO00000002' or both");
  }

  @Test
  void whenEitherParentOrChildAccountNotFoundOnAccountHierarchyWithAndWithoutCancellationFlag() {
    val result =
        deriveAndValidateAccountForHierarchyUpdates(
                datasetOf(
                    PARTY_ENCODER,
                    PARTY_PO00000001_WITH_CANCELLEDFUND_ACCOUNTS,
                    PARTY_PO00000002_WITH_FUND_ACCOUNTS),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG))
            .collectAsList();

    assertErrorUpdates(
        result,
        2,
        "account not found either for parentPartyId 'PO00000001' childPartyId 'PO00000002' or both",
        "account not found either for parentPartyId 'PO00000001' childPartyId 'PO00000002' or both");
  }

  @Test
  void whenMultipleValidAndCancellationHierUpdatesWithCorrectAccount() {
    val result =
        deriveAndValidateAccountForHierarchyUpdates(
                datasetOf(
                    PARTY_ENCODER,
                    PARTY_PO00000001_WITH_FUND_ACCOUNTS,
                    PARTY_PO00000002_WITH_FUND_ACCOUNTS),
                datasetOf(
                    ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                    ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG))
            .collectAsList();

    assertSuccessUpdates(
        result,
        2,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CANCEL_FLG);
  }

  @Test
  void whenNewAccountHierarchy() {
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002));

    assertErrors(result, 0);
    assertSuccess(result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameParentAndDifferentChild() {
    // B -> D 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op
    // B -> D 21-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
  }

  @Test
  void whenSameNewAccountHierarchyWithSuccessValidity() {
    // A -> B 01-Jan 31-Jan
    // A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb null(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenSameNewHierarchyWithCrossOverValidity() {
    // A -> B 01-Jan null
    // A -> B 01-Feb null
    // op
    // A -> B 01-Jan null(error)
    // A -> B 01-Feb null(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE));

    assertErrors(
        result,
        2,
        "incorrect records federated having validity cross over for same hierarchy ['PO00000001', 'PO00000002']");
    assertSuccess(result, 0);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithChildComesAsParentWithSuccessValidity() {
    // A -> B 01-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op -
    // A -> B 01-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithChildComesAsParentAndCrossOverValidity() {
    // A -> B 01-Jan 31-Jan
    // B -> D 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op -
    // A -> B 01-Jan 31-Jan(error)
    // B -> C 21-Jan 31-Jan(error)
    // B -> D 01-Feb 28-Feb(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004));

    assertErrors(
        result,
        3,
        "incorrect records federated for cross level hierarchy ['PO00000001', 'PO00000002'], ['PO00000002', 'PO00000004']");
    assertSuccess(result, 0);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithParentComesAsChildUpdateWithSuccessValidity() {
    // C -> E 01-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op -
    // C -> E 01-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithParentComesAsChildAndCrossOverValidity() {
    // C -> E 01-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // D -> C 21-Jan 31-Jan
    // op -
    // C -> E 01-Jan 31-Jan(error)
    // B -> C 01-Feb 28-Feb(error)
    // D -> C 21-Jan 31-Jan(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003));

    assertErrors(
        result,
        3,
        "incorrect records federated for cross level hierarchy ['PO00000003', 'PO00000005'], ['PO00000004', 'PO00000003']");
    assertSuccess(result, 0);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameChildAndDifferentParentWithSuccessValidity() {
    // D -> C 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // op
    // D -> C 21-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
  }

  @Test
  void whenMultipleNewAccountHierarchyWithSameChildAndDifferentParentAndCrossOverValidity() {
    // D -> C 21-Jan 31-Jan
    // B -> C 01-Feb 28-Feb
    // F -> C 25-Jan 25-Feb
    // op
    // D -> C 21-Jan 31-Jan(error)
    // B -> C 01-Feb 28-Feb(error)
    // F -> C 25-Jan 25-Feb(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(ACCOUNT_HIERARCHY_ENCODER),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000006_AND_PO00000003));

    assertErrors(
        result,
        3,
        "incorrect records federated with child 'PO00000003' having multiple parents ['PO00000004', 'PO00000003'], ['PO00000006', 'PO00000003'], ['PO00000002', 'PO00000003']");
    assertSuccess(result, 0);
  }

  @Test
  void whenNoUpdateForActiveAccountHierarchy() {
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002),
            datasetOf(ACCOUNT_HIERARCHY_DATA_ROW_ENCODER));

    assertErrors(result, 0);
    assertSuccess(result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenPartialValidUpdateForActiveAccountHierarchyWithSameParentAndDifferentChild() {
    // current B -> C 01-Feb 28-Feb
    // update  B -> C 01-Mar null
    // update  B -> D 21-Jan 31-Jan
    // op
    // B -> C 01-Feb 28-Feb(success)
    // B -> C 01-Mar null(success)
    // B -> D 21-Jan 31-Jan(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003_UPDATE));

    assertSuccess(
        result,
        3,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003_AND_NEW_VALIDITY,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000004);
  }

  @Test
  void whenSameAccountHierarchyUpdateForActiveHierarchyWithSuccessValidity() {
    // current A -> B 01-Jan 31-Jan
    // update A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb null
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenSameAccountHierarchyUpdateForActiveHierarchyWithSameAndDifferentValidFrom() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan
    // update A -> B 01-Feb null
    // op
    // A -> B 01-Jan 31-Jan(updated)
    // A -> B 01-Feb null(new)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenMultipleSameAccountHierarchyAndUpdateForLatestActiveHierarchyWithSameValidFrom() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 01-Feb 28-Feb
    // op
    // A -> B 01-Jan 31-Jan(success)
    // A -> B 01-Feb 28-Feb(updated)

    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_FEB_VALIDITY_EXPIRED);
  }

  @Test
  void
      whenMultipleSameAccountHierarchyAndMultipleUpdateForLatestActiveHierarchyWithCrossOverValidity() {
    // current A -> B 01-Jan 31-Jan
    // current A -> B 01-Feb null
    // update A -> B 21-Jan 31-Jan
    // update A -> B 01-Feb 28-Feb
    // op
    // current A -> B 01-Jan 31-Jan(success)
    // current A -> B 01-Feb 28-Feb(success)
    // update A -> B 21-Jan 31-Jan(error)
    // update A -> B 01-Feb 28-Feb(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_CROSS_OVER_VALIDITY,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_FEB_VALIDITY_EXPIRED));

    assertErrors(
        result,
        2,
        "active hierarchy ['PO00000001', 'PO00000002'] has validity cross over with same hierarchy in updates");
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_NEW_VALIDITY);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithChildComesAsParentAndSuccessValidity() {
    // current A -> B 01-Jan 31-Jan
    // update B -> C 01-Feb 28-Feb
    // op
    // A -> B 01-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithChildComesAsParentAndCrossOverValidity() {
    // current - A -> B 01-Jan 31-Jan
    // current - G -> B 01-Feb 28-Feb
    // update - B -> C 01-Feb 28-Feb
    // update - B -> D 21-Jan 31-Jan
    // op
    // A -> B 01-Jan 31-Jan(success)
    // G -> B 01-Feb 28-Feb(success)
    // B -> C 01-Feb 28-Feb(error)
    // B -> D 21-Jan 31-Jan(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000007_AND_PO00000002),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004));

    assertErrors(
        result,
        2,
        "child of active hierarchy ['PO00000001', 'PO00000002'] is federated as parent in updates ['PO00000002', 'PO00000004']",
        "child of active hierarchy ['PO00000007', 'PO00000002'] is federated as parent in updates ['PO00000002', 'PO00000003'], ['PO00000002', 'PO00000004']");
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000007_AND_PO00000002);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithParentComesAsChildWithSuccessValidity() {
    // current - C -> E 01-Jan 31-Jan
    // update - B -> C 01-Feb 28-Feb
    // op
    // C -> E 01-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithParentComesAsChildAndCrossOverValidity() {
    // current - C -> E 01-Jan 31-Jan
    // current - C -> H 01-Feb 28-Feb
    // update - B -> C 01-Feb 28-Feb
    // update - D -> C 21-Jan 31-Jan
    // op
    // C -> E 01-Jan 31-Jan(success)
    // C -> H 01-Feb 28-Feb(success)
    // D -> C 21-Jan 31-Jan(error)
    // B -> C 01-Feb 28-Feb(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000008),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003));

    assertErrors(
        result,
        2,
        "parent of active hierarchy ['PO00000003', 'PO00000005'] is federated as child in updates ['PO00000004', 'PO00000003']",
        "parent of active hierarchy ['PO00000003', 'PO00000008'] is federated as child in updates ['PO00000002', 'PO00000003'], ['PO00000004', 'PO00000003']");
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000005,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000008);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithSameChildButDifferentParentWithSuccessValidity() {
    // current - D -> C 21-Jan 31-Jan
    // update - B -> C 01-Feb 28-Feb
    // op
    // D -> C 21-Jan 31-Jan(success)
    // B -> C 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
  }

  @Test
  void whenHierarchyIsUpdatedAndChildIsMovedToDifferentParentInSingleRun() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan
    // update C -> B 01-Feb 28-Feb
    // op
    // A -> B 01-Jan 31-Jan(success)
    // C -> B 01-Feb 28-Feb(success)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_JAN_VALIDITY_EXPIRED,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000003_AND_PO00000002));

    assertErrors(result, 0);
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATED_AND_JAN_VALIDITY_EXPIRED,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000003_AND_PO00000002);
  }

  @Test
  void whenUpdateForActiveAccountHierarchyWithSameChildButDifferentParentAndCrossOverValidity() {
    // current - F -> C 25-Jan 25-Feb
    // update - D -> C 21-Jan 31-Jan
    // update - B -> C 01-Feb 28-Feb
    // op
    // F -> C 25-Jan 25-Feb(success)
    // D -> C 21-Jan 31-Jan(error)
    // B -> C 01-Feb 28-Feb(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000004_AND_PO00000003));

    assertErrors(
        result,
        2,
        "child of active hierarchy ['PO00000006', 'PO00000003'] is federated with multiple parents in updates ['PO00000004', 'PO00000003']");
    assertSuccess(result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000006_AND_PO00000003);
  }

  @Test
  void whenUnhandledErrorValidatingUpdatesThenTaskDoesNotFailAndUpdateIsMarkedAsFailed() {
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_INVALID_UPDATE_NULL_POINTER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000003));

    assertThat(result.getError().collectAsList()).hasSize(2);
    assertSuccess(result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002);
  }

  @Test
  void whenUnhandledErrorUpdatingActiveHierarchyThenTaskDoesNotFailAndUpdatesAreMarkedAsFailed() {
    // current A -> B 01-Jan null
    // update A -> B 01-Jan 31-Jan(invalid)
    // op
    // A -> B 01-Jan null(success)
    // A -> B 01-Jan 31-Jan(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000001_AND_PO00000002_UPDATE_AND_TXN_HEADER_ID_NULL));

    assertThat(
            List.ofAll(result.getUpdateResult().collectAsList())
                .map(AccountHierarchyUpdateResult::getErrors))
        .hasSize(1);
    assertSuccess(
        result, 1, ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002_AND_VALID_TO_NULL);
  }

  @Test
  void whenTwoLevelHierarchyExistBetweenActiveHierarchyAndUpdatesAndCrossOverValidity() {
    // current A -> B 01-Jan 31-Jan
    // current D -> C 21-Jan 31-Jan
    // update B -> D 21-Jan 31-Jan
    // op
    // A -> B 01-Jan 31-Jan(success)
    // D -> C 21-Jan 31-Jan(success)
    // B -> D 21-Jan 31-Jan(error)
    val result =
        mapCurrentAccountHierarchyAndUpdatesToSuccessOrError(
            datasetOf(
                ACCOUNT_HIERARCHY_ENCODER,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
                ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003),
            datasetOf(
                ACCOUNT_HIERARCHY_DATA_ROW_ENCODER,
                ACCOUNT_HIERARCHY_DATA_ROW_WITH_PARTY_IDS_PO00000002_AND_PO00000004));

    assertErrors(
        result,
        1,
        "child of active hierarchy ['PO00000001', 'PO00000002'] is federated as parent in updates ['PO00000002', 'PO00000004']",
        "parent of active hierarchy ['PO00000004', 'PO00000003'] is federated as child in updates ['PO00000002', 'PO00000004']");
    assertSuccess(
        result,
        2,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000001_AND_PO00000002,
        ACCOUNT_HIERARCHY_WITH_PARTY_IDS_PO00000004_AND_PO00000003);
  }

  private void assertSuccessUpdates(
      java.util.List<AccountHierarchyValidationResult> result,
      int count,
      AccountHierarchyDataRow... updates) {
    val success =
        List.ofAll(result)
            .filter(AccountHierarchyValidationResult::isSuccess)
            .map(AccountHierarchyValidationResult::getUpdate);

    assertThat(success).hasSize(count);
    assertThat(success).containsExactlyInAnyOrder(updates);
  }

  private void assertErrorUpdates(
      java.util.List<AccountHierarchyValidationResult> result, int count, String... messages) {
    val errors =
        List.ofAll(result)
            .filter(AccountHierarchyValidationResult::isError)
            .map(AccountHierarchyValidationResult::getError);

    assertThat(errors).hasSize(count);
    assertThat(errors.map(ErrorAccountHierarchy::getMessage).mkString("\n"))
        .isEqualTo(List.of(messages).mkString("\n"));
  }

  private void assertErrors(AccountHierarchyResult result, int count, String... messages) {
    List<ErrorAccountHierarchy> hierarchyErrors =
        List.ofAll(result.getUpdateResult().collectAsList())
            .flatMap(updateResult -> List.of(updateResult.getErrors()));
    val errors = List.ofAll(result.getError().collectAsList()).appendAll(hierarchyErrors);

    assertThat(errors).hasSize(count);
    errors.forEach(
        error -> assertThat(error.getMessage()).isEqualTo(List.of(messages).mkString("\n")));
  }

  private void assertSuccess(
      AccountHierarchyResult result, int count, AccountHierarchy... hierarchies) {
    List<AccountHierarchy> success =
        List.ofAll(result.getUpdateResult().collectAsList())
            .flatMap(updateResult -> List.of(updateResult.getSuccess()));
    assertThat(success).hasSize(count);
    // ignore comparing creationDate and update flg
    assertThat(success)
        .usingElementComparatorIgnoringFields(UPDATE_FLG_AND_CREATION_DATE)
        .containsExactlyInAnyOrder(hierarchies);
  }
}
