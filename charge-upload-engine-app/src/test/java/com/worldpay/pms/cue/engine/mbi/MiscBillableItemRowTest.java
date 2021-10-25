package com.worldpay.pms.cue.engine.mbi;

import static com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow.getCaseFlag;
import static com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow.getHashString;
import static com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow.getSourceId;
import static com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow.getSourceType;
import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.cue.engine.utils.WithDatabase;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class MiscBillableItemRowTest implements WithDatabase {
  @Test
  void testCaseFlag() {
    assertThat(getCaseFlag(null, null, null)).isEqualTo("N");
    assertThat(getCaseFlag(null, null, "N")).isEqualTo("N");
    assertThat(getCaseFlag(null, "N", "N")).isEqualTo("N");

    assertThat(getCaseFlag("Y", null, null)).isEqualTo("N");
    assertThat(getCaseFlag("Y", null, "N")).isEqualTo("N");

    assertThat(getCaseFlag("N", null, "Y")).isEqualTo("N");
    assertThat(getCaseFlag("N", "Y", null)).isEqualTo("N");

    assertThat(getCaseFlag("Y", null, "Y")).isEqualTo("Y");
    assertThat(getCaseFlag("Y", "Y", null)).isEqualTo("Y");
  }

  @Test
  void testHashString() {
    assertThat(getHashString(Stream.of("Y", "N", "N", null, "12345"))).isEqualTo("YNN12345");
  }

  @Test
  void testGetSourceType() {
    assertThat(getSourceType(null, null)).isEqualTo(null);

    assertThat(getSourceType("12345", null)).isEqualTo("EVENT");
    assertThat(getSourceType("12345", "ADJUSTMENT")).isEqualTo("EVENT");

    assertThat(getSourceType(null, "ADJUSTMENT")).isEqualTo("ADJUSTMENT");
    assertThat(getSourceType(null, "")).isEqualTo("");
    assertThat(getSourceType(null, " ")).isEqualTo(" ");
  }

  @Test
  void testGetSourceId() {
    assertThat(getSourceId(null, null)).isEqualTo(null);
    assertThat(getSourceId("12345", null)).isEqualTo("12345");
    assertThat(getSourceId("12345", "12345SRC")).isEqualTo("12345");
    assertThat(getSourceId(null, "12345SRC")).isEqualTo("12345SRC");
    assertThat(getSourceId(null, "")).isEqualTo("");
    assertThat(getSourceId(null, " ")).isEqualTo(" ");
  }
}
