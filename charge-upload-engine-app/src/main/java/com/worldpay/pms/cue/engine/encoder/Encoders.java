package com.worldpay.pms.cue.engine.encoder;

import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.Encoders.bean;
import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.Encoders.tuple;

import com.worldpay.pms.cue.engine.mbi.MiscBillableItem;
import com.worldpay.pms.cue.engine.pbc.ErrorTransaction;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
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
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

public class Encoders {

  public static final Encoder<PendingBillableChargeRow> PENDING_BILLABLE_CHARGE_ROW_ENCODER =
      bean(PendingBillableChargeRow.class);
  public static final Encoder<MiscBillableItem> MISC_BILLABLE_ITEM_ENCODER =
      bean(MiscBillableItem.class);
  public static final Encoder<RecurringResultRow> RECURRING_RESULT_ROW_ENCODER =
      bean(RecurringResultRow.class);
  public static final Encoder<VwmBcuAccountRow> VWM_BCU_ACCOUNT_ROW_ENCODER =
      bean(VwmBcuAccountRow.class);
  public static final Encoder<RecurringIdentifierRow> RECURRING_IDENTIFIER_ROW_ENCODER =
      bean(RecurringIdentifierRow.class);
  public static final Encoder<RecurringCharge> RECURRING_CHARGE_ENCODER =
      kryo(RecurringCharge.class);
  public static final Encoder<IntermediateRecurringCharge> INTERMEDIATE_RECURRING_CHARGE_ENCODER =
      kryo(IntermediateRecurringCharge.class);
  public static final Encoder<RecurringIdentifierWrapper> RECURRING_IDENTIFIER_WRAPPER_ENCODER =
      kryo(RecurringIdentifierWrapper.class);
  public static final Encoder<RecurringIdentifierKey> RECURRING_IDENTIFIER_KEY_ENCODER =
      kryo(RecurringIdentifierKey.class);
  public static final Encoder<RecurringErrorTransaction> RECURRING_ERROR_TRANSACTION_ENCODER =
      kryo(RecurringErrorTransaction.class);
  public static final Encoder<RecurringChargeRow> RECURRING_CHARGE_ROW_ENCODER =
      bean(RecurringChargeRow.class);
  public static final Encoder<SuccessOrFailureNonRecurring>
      CHARGED_AND_FAILED_TRANSACTIONS_ENCODERS = kryo(SuccessOrFailureNonRecurring.class);
  public static final Encoder<SuccessOrFailureRecurring> SUCCESS_RECURRING_OR_ERROR_ENCODER =
      kryo(SuccessOrFailureRecurring.class);
  public static final Encoder<SuccessOrFailureRecurringCharge>
      SUCCESS_OR_FAILURE_RECURRING_CHARGE_ENCODER = kryo(SuccessOrFailureRecurringCharge.class);
  public static final Encoder<ChargedTransaction> CHARGED_TRANSACTION_ENCODER =
      kryo(ChargedTransaction.class);
  public static final Encoder<ErrorTransaction> ERROR_TRANSACTION_ENCODER =
      kryo(ErrorTransaction.class);
  public static final Encoder<Tuple2<String, MiscBillableItem>> MISC_BILLABLE_ITEM_WITH_ID_ENCODER =
      tuple(STRING(), MISC_BILLABLE_ITEM_ENCODER);

  private Encoders() {}
}
