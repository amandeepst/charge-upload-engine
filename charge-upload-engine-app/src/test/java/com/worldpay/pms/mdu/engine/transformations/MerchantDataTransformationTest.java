package com.worldpay.pms.mdu.engine.transformations;

import static com.worldpay.pms.mdu.engine.encoder.InputEncoders.MERCHANT_DATA_ENCODER;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHBK;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHRG;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CRWD;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_PARTY_PO00000001;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS;
import static com.worldpay.pms.mdu.engine.samples.Transactions.MERCHANT_WITH_PARTY_PO00000001;
import static com.worldpay.pms.mdu.engine.transformations.MerchantDataTransformation.mapMerchantDataToMerchantDataPartyRow;
import static com.worldpay.pms.testing.junit.SparkContextHeavyUsage.datasetOf;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import lombok.val;
import org.junit.jupiter.api.Test;

@WithSparkHeavyUsage
public class MerchantDataTransformationTest {

  @Test
  void whenNoMerchantDataIsStaged() {
    val result =
        mapMerchantDataToMerchantDataPartyRow(datasetOf(MERCHANT_DATA_ENCODER)).collectAsList();
    assertThat(result).hasSize(0);
  }

  @Test
  void whenMerchantDataWithNoAccountsIsStaged() {
    val result =
        mapMerchantDataToMerchantDataPartyRow(
                datasetOf(MERCHANT_DATA_ENCODER, MERCHANT_WITH_PARTY_PO00000001))
            .collectAsList();

    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(MERCHANT_PARTY_PO00000001);
  }

  @Test
  void whenMerchantDataWithAccountsIsStaged() {
    val result =
        mapMerchantDataToMerchantDataPartyRow(
                datasetOf(
                    MERCHANT_DATA_ENCODER,
                    MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_FUND,
                    MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHRG,
                    MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CHBK,
                    MERCHANT_DATA_WITH_PARTY_PO00000001_AND_ACCOUNT_CRWD))
            .collectAsList();

    assertThat(result).hasSize(1);
    assertThat(result.get(0))
        .usingRecursiveComparison()
        .ignoringOverriddenEqualsForFields("accounts")
        .isEqualTo(MERCHANT_PARTY_PO00000001_WITH_ALL_VALID_ACCOUNTS);
  }
}
