package com.worldpay.pms.cue.domain;

import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;

@Value
public class Currency implements Serializable {

  @NonNull String currencyCode;
  short roundingScale;
}
