package com.worldpay.pms.cue.engine.mbi;

import static com.worldpay.pms.cue.domain.common.LineCalculationType.PI_RECUR;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import java.math.BigDecimal;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MiscBillableItemLineRow {

  @NonNull String billableItemLineId;
  @NonNull String lineCalculationType;
  @NonNull BigDecimal lineAmount;
  @NonNull BigDecimal price;
  @NonNull String currency;
  @NonNull long quantity;

  public static MiscBillableItemLineRow build(Charge charge, String currency, long quantity) {
    return new MiscBillableItemLineRow(
        UUID.randomUUID().toString(),
        charge.getLineCalcType(),
        charge.getLineAmount(),
        charge.getLineAmount(),
        currency,
        quantity);
  }

  public static MiscBillableItemLineRow build(
      BigDecimal lineAmount, BigDecimal price, String currency, long quantity) {
    return new MiscBillableItemLineRow(
        UUID.randomUUID().toString(), PI_RECUR.name(), lineAmount, price, currency, quantity);
  }
}
