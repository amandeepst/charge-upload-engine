package com.worldpay.pms.cue.engine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigDecimalSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptySetSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.DefaultChargingService;
import com.worldpay.pms.cue.domain.PendingBillableChargeError;
import com.worldpay.pms.cue.domain.validator.RawValidationService;
import com.worldpay.pms.cue.domain.validator.RawValidationService.NonRecurringChargeValidator;
import com.worldpay.pms.cue.domain.validator.RawValidationService.RecurringChargeAccountValidator;
import com.worldpay.pms.cue.domain.validator.RawValidationService.RecurringChargeValidator;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemLineRow;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemRow;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorTransaction;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import com.worldpay.pms.cue.engine.transformations.ChargedTransaction;
import com.worldpay.pms.cue.engine.transformations.IntermediateRecurringCharge;
import com.worldpay.pms.cue.engine.transformations.RecurringCharge;
import com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierKey;
import com.worldpay.pms.cue.engine.transformations.SafeCheckUtils.RecurringIdentifierWrapper;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureNonRecurring;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureRecurring;
import com.worldpay.pms.cue.engine.transformations.SuccessOrFailureRecurringCharge;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountRow;
import com.worldpay.pms.spark.KryoConfiguration;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import lombok.SneakyThrows;

public class ChargingKryoRegistrator extends KryoConfiguration {
  @SneakyThrows
  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    // base
    kryo.register(BigDecimal.class, new BigDecimalSerializer());
    kryo.register(Collections.emptySet().getClass(), new CollectionsEmptySetSerializer());
    kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
    kryo.register(HashSet.class);

    // domain
    kryo.register(DefaultChargingService.class);
    kryo.register(RawValidationService.class);
    kryo.register(RecurringChargeValidator.class);
    kryo.register(NonRecurringChargeValidator.class);
    kryo.register(RecurringChargeAccountValidator.class);
    kryo.register(ChargingService.Charge.class);
    kryo.register(PendingBillableChargeError.class);

    // output
    kryo.register(RecurringResultRow.class);
    kryo.register(RecurringChargeRow.class);
    kryo.register(SuccessOrFailureNonRecurring.class);
    kryo.register(SuccessOrFailureRecurring.class);
    kryo.register(SuccessOrFailureRecurringCharge.class);
    kryo.register(ChargedTransaction.class);
    kryo.register(MiscBillableItem.class);
    kryo.register(MiscBillableItemRow.class);
    kryo.register(MiscBillableItemLineRow.class);
    kryo.register(ErrorTransaction.class);
    kryo.register(RecurringErrorTransaction.class);
    kryo.register(VwmBcuAccountRow.class);
    kryo.register(RecurringIdentifierRow.class);
    kryo.register(RecurringCharge.class);
    kryo.register(IntermediateRecurringCharge.class);
    kryo.register(RecurringIdentifierKey.class);
    kryo.register(RecurringIdentifierWrapper.class);

    // additional kryo registry for persisting PendingBillableChargeRow
    kryo.register(org.apache.spark.sql.types.Decimal.class);
    kryo.register(org.apache.spark.unsafe.types.UTF8String.class);
    kryo.register(scala.math.BigDecimal.class);
    kryo.register(java.math.MathContext.class);
    kryo.register(java.math.RoundingMode.class);
  }
}
