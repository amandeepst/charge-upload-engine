package com.worldpay.pms.cue.engine;

import com.beust.jcommander.Parameter;
import com.typesafe.config.Config;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.cli.converter.LogicalDateConverter;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.cue.domain.ChargingService;
import com.worldpay.pms.cue.domain.Currency;
import com.worldpay.pms.cue.domain.Product;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.ChargingRepositoryConfiguration;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.DataSources;
import com.worldpay.pms.cue.engine.ChargingUploadConfig.DataWriters;
import com.worldpay.pms.cue.engine.batch.ChargingBatchHistoryRepository;
import com.worldpay.pms.cue.engine.batch.ChargingBatchRunResult;
import com.worldpay.pms.cue.engine.common.Factory;
import com.worldpay.pms.cue.engine.mbi.MiscBillableItemWriter;
import com.worldpay.pms.cue.engine.pbc.ErrorTransactionWriter;
import com.worldpay.pms.cue.engine.pbc.PendingBillableChargeSource;
import com.worldpay.pms.cue.engine.recurring.ErrorRecurringChargeSource;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeAuditWriter;
import com.worldpay.pms.cue.engine.recurring.RecurringChargeWriter;
import com.worldpay.pms.cue.engine.recurring.RecurringErrorWriter;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierSource;
import com.worldpay.pms.cue.engine.recurring.RecurringIdentifierWriter;
import com.worldpay.pms.cue.engine.recurring.RecurringResultSource;
import com.worldpay.pms.cue.engine.staticdata.JdbcCurrencyRepository;
import com.worldpay.pms.cue.engine.staticdata.JdbcProductRepository;
import com.worldpay.pms.cue.engine.staticdata.JdbcStaticDataRepository.RepositoryFactory;
import com.worldpay.pms.cue.engine.vwm.VwmBcuAccountSource;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import io.vavr.Function1;
import io.vavr.Lazy;
import io.vavr.control.Option;
import java.sql.Date;
import java.time.LocalDate;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ChargingUploadEngine extends SparkApp<ChargingUploadConfig, ChargingBatchRunResult> {

  private final Lazy<ChargingBatchHistoryRepository> batchHistory;
  final Function<BatchId, Long> generateRunId = Function1.of(this::getRunId).memoized();

  private Option<Long> runId = Option.none();

  @Getter protected Date logicalDate;

  public ChargingUploadEngine(Config config) {
    super(config, ChargingUploadConfig.class);
    this.batchHistory = Lazy.of(this::buildBatchHistory);
  }

  public static void main(String... args) {
    SparkApp.run(ChargingUploadEngine::new, args);
  }

  @Override
  public PmsParameterDelegate getParameterDelegate() {
    return new ChargingEngineParameterDelegate();
  }

  @Override
  public void parseExtraArgs(PmsParameterDelegate delegate) {
    if (delegate instanceof ChargingEngineParameterDelegate) {
      ChargingEngineParameterDelegate pEDelegate = (ChargingEngineParameterDelegate) delegate;
      runId = Option.of(pEDelegate.runId);
      logicalDate = Date.valueOf(pEDelegate.logicalDate);
    } else {
      throw new PMSException("Parameter delegate corrupted. Impossible state.");
    }
  }

  @Override
  protected ChargingBatchHistoryRepository buildBatchHistoryRepository(
      ApplicationConfiguration conf, ChargingUploadConfig settings) {
    return batchHistory.get();
  }

  @Override
  protected JavaDriver<ChargingBatchRunResult> buildDriver(
      SparkSession sparkSession, ChargingUploadConfig settings) {
    DataSources sources = settings.getSources();
    DataWriters writers = settings.getWriters();
    return new Driver(
        sparkSession,
        generateRunId,
        getLogicalDate(),
        getChargingService(sources.getStaticData()),
        new PendingBillableChargeSource(sources.getChargeEvents(), settings.getMaxAttempts()),
        new RecurringResultSource(sources.getRecurringSource(), getLogicalDate()),
        new VwmBcuAccountSource(sources.getVwmBcuAccountSource()),
        new RecurringIdentifierSource(sources.getRecurringIdentifierSource()),
        new ErrorRecurringChargeSource(
            sources.getErrorRecurringChargeSource(), settings.getMaxAttempts()),
        new RecurringChargeWriter(writers.getTransitionalRecurringCharges()),
        new RecurringErrorWriter(writers.getRecurringErrorCharges(), sparkSession),
        new RecurringChargeAuditWriter(writers.getTransitionalRecurringChargesAudit()),
        new MiscBillableItemWriter(
            writers.getBillableCharges(), getCurrencies(settings), getProducts(settings)),
        new RecurringIdentifierWriter(writers.getRecurringIdentifiers()),
        new ErrorTransactionWriter(writers.getFailedTransactions(), sparkSession));
  }

  protected Factory<ChargingService> getChargingService(ChargingRepositoryConfiguration conf) {
    return new TransactionChargingServiceFactory(new RepositoryFactory(conf));
  }

  private Iterable<Currency> getCurrencies(ChargingUploadConfig settings) {
    return new JdbcCurrencyRepository(settings.getSources().getStaticData().getConf())
        .getCurrencies();
  }

  private Iterable<Product> getProducts(ChargingUploadConfig settings) {
    return new JdbcProductRepository(settings.getSources().getStaticData().getConf()).getProducts();
  }

  private Long getRunId(BatchId batchId) {
    return runId.getOrElse(() -> batchHistory.get().generateRunId(batchId));
  }

  protected ChargingBatchHistoryRepository buildBatchHistory() {
    return new ChargingBatchHistoryRepository(getConf().getDb(), getSettings().getPublishers());
  }

  private static class ChargingEngineParameterDelegate implements PmsParameterDelegate {

    @Parameter(
        names = {"--run-id"},
        arity = 1,
        description = "Force a particular run id rather than generating one",
        required = false)
    private Long runId;

    @Parameter(
        names = {"--logical-date"},
        arity = 1,
        description = "Date when the txns happened.",
        required = true,
        converter = LogicalDateConverter.class)
    private LocalDate logicalDate;
  }
}
