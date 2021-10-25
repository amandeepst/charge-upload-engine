package com.worldpay.pms.mdu.engine.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.MerchantUploadConfig;
import com.worldpay.pms.mdu.engine.data.JdbcLcpDetailsRepository.LcpDescription;
import com.worldpay.pms.mdu.engine.utils.DatabaseCsvUtils.Database;
import com.worldpay.pms.mdu.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import io.vavr.collection.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JdbcLcpDetailsRepositoryTest implements WithDatabaseAndSpark {
  private Database cisadm;
  private JdbcLcpDetailsRepository target;
  private static final String ROOT = "input/JdbcStaticRepositoryTest/";

  @Override
  public void bindMerchantUploadConfiguration(MerchantUploadConfig settings) {
    this.target = new JdbcLcpDetailsRepository(settings.getSources().getStaticData().getConf());
  }

  @Override
  public void bindOrmbJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadm = new Database(conf, ROOT);
  }

  @Test
  void canLoadLcpDescription() {
    cisadm.run(
        () -> {
          Map<String, String> lcpDescriptions = target.getLcpDescription();
          assertThat(lcpDescriptions.get("00014")).isEqualTo("Worldpay H.K.");
        });
  }

  @Test
  void sonarCoverageFix() {

    Map<String, String> lcpDescriptions =
        target.tolcpDescriptionsArray(
            List.of(
                    new LcpDescription("", ""),
                    new LcpDescription("    ", "    "),
                    new LcpDescription(null, null),
                    new LcpDescription(null, " "),
                    new LcpDescription(" ", null),
                    new LcpDescription("00014", "Worldpay H.K."),
                    new LcpDescription(" 00015", " Worldpay H.K."))
                .toJavaList());
    assertThat(lcpDescriptions).hasSize(2);
  }
}
