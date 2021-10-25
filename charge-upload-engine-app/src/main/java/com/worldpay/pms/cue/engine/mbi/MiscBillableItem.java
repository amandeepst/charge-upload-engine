package com.worldpay.pms.cue.engine.mbi;

import static scala.Tuple2.apply;

import com.worldpay.pms.cue.domain.ChargingService.Charge;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeRow;
import com.worldpay.pms.cue.engine.recurring.RecurringResultRow;
import io.vavr.collection.Stream;
import java.sql.Date;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.TaskContext;
import org.hashids.Hashids;
import scala.Tuple2;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MiscBillableItem {
  // character `?` reserved for emergencies
  private static final String ALPHABET =
      "!$%*+-0123456789:;<=>@ACDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz~#";
  /**
   * adding 'U' prefix with hashid generated, considering batch size is small which will limit to
   * generate id with maxLength as 11, we can switch to other unique id generation logic once
   * billing is refactored
   */
  private static final Hashids hashids = new Hashids("", 11, ALPHABET);

  private static final String ID_PREFIX = "U";

  MiscBillableItemRow miscBillableItemRow;
  MiscBillableItemLineRow miscBillableItemLineRow;

  public static MiscBillableItem build(
      PendingBillableChargeRow nonRecurringRow, Charge charge, Date logicalDate) {
    return new MiscBillableItem(
        MiscBillableItemRow.build(nonRecurringRow, logicalDate),
        MiscBillableItemLineRow.build(
            charge, nonRecurringRow.getCurrency(), nonRecurringRow.getQuantity()));
  }

  public static MiscBillableItem build(RecurringResultRow recurringRow, Charge charge) {
    return new MiscBillableItem(
        MiscBillableItemRow.build(recurringRow),
        MiscBillableItemLineRow.build(
            charge.getLineAmount(),
            recurringRow.getPrice(),
            recurringRow.getCurrency(),
            recurringRow.getQuantity()));
  }

  public static Iterator<Tuple2<String, MiscBillableItem>> generateBillItemId(
      long runId, Iterator<MiscBillableItem> partition) {
    return Stream.ofAll(() -> partition)
        .zipWithIndex()
        .map(t -> apply(generateBillItemId(runId, TaskContext.getPartitionId(), t._2), t._1))
        .iterator();
  }

  public static String generateBillItemId(long runId, int partitionId, long idx) {
    return ID_PREFIX + hashids.encode(runId, partitionId, idx);
  }
}
