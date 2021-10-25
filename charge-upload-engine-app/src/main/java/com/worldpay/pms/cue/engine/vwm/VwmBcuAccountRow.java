package com.worldpay.pms.cue.engine.vwm;

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
public class VwmBcuAccountRow {

  String partyId;
  String currencyCode;
  String accountId;
  String subaccountId;
  String legalcounterparty;
  String subaccountType;
  long partitionId;
}
