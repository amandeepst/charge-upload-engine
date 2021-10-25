package com.worldpay.pms.mdu.engine.transformations;

import static org.assertj.core.api.Assertions.assertThat;

import com.worldpay.pms.mdu.engine.transformations.model.input.PartyHierarchyDataRow;
import com.worldpay.pms.pce.common.DomainError;
import org.junit.jupiter.api.Test;

public class ErrorPartyHierarchyTest {

  @Test
  void testErrorPartyHierarchy() {

    PartyHierarchyDataRow dataRow1 =
        PartyHierarchyDataRow.builder()
            .childPartyId("child")
            .parentPartyId("parent")
            .txnHeaderId("11111")
            .build();

    PartyHierarchyDataRow dataRow2 =
        PartyHierarchyDataRow.builder()
            .childPartyId("child2")
            .parentPartyId("parent2")
            .txnHeaderId("22222")
            .build();

    DomainError de = DomainError.of("CODE", "ERROR");
    ErrorPartyHierarchy e1 = ErrorPartyHierarchy.of(dataRow1, de);
    ErrorPartyHierarchy e2 = ErrorPartyHierarchy.of(dataRow2, de);

    assertThat(ErrorPartyHierarchy.concat(e1, e2)).isNotNull();
  }
}
