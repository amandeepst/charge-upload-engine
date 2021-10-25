package com.worldpay.pms.mdu.engine.utils;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Array;
import io.vavr.collection.HashSet;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Query;
import org.sql2o.data.Column;

public class DatabaseCsvUtils {

  public static void readFromCsvFileAndWriteToExistingTable(
      SqlDb db, String fileName, String tableName) {
    AtomicInteger lineNumber = new AtomicInteger(0);
    try {
      List<Record> records = getCsvRecords(fileName);
      records.remove(0); // remove header record

      Seq<Column> columns =
          db.execQuery(
              "get-table-metadata",
              format("SELECT * FROM %s WHERE 1=0", tableName),
              q -> Array.ofAll(q.executeAndFetchTable().columns()));

      db.exec(
          "insert " + tableName,
          c -> {
            records.forEach(
                record -> {
                  lineNumber.incrementAndGet();
                  c.createQuery(createInsertStatement(tableName, columns, record)).executeUpdate();
                });

            return true;
          });
    } catch (Exception e) {
      throw new RuntimeException("Error performing insert at line: " + lineNumber.get(), e);
    }
  }

  private static String createInsertStatement(
      String tableName, Seq<Column> columns, Record record) {
    Seq<Tuple2<String, String>> values =
        columns
            .filter(column -> record.getMetaData().containsColumn(column.getName()))
            .map(
                column -> {
                  String name = column.getName();
                  String value = record.getString(name);
                  if (value == null || "null".equalsIgnoreCase(value))
                    return Tuple.of(name, "NULL");

                  switch (column.getType().toLowerCase()) {
                    case "date":
                      return Tuple.of(name, format("DATE '%s'", Date.valueOf(value)));
                    case "timestamp":
                      try {
                        return Tuple.of(name, format("TIMESTAMP '%s'", Timestamp.valueOf(value)));
                      } catch (IllegalArgumentException e) {
                        return Tuple.of(
                            name,
                            format(
                                "TIMESTAMP '%s'",
                                Timestamp.valueOf(
                                    Date.valueOf(value).toLocalDate().atStartOfDay())));
                      }
                    default:
                      return Tuple.of(name, format("'%s'", value));
                  }
                });

    return format(
        "INSERT INTO %s (%s) VALUES (%s)",
        tableName, values.map(Tuple2::_1).mkString(","), values.map(Tuple2::_2).mkString(","));
  }

  public static <T> void checkThatDataFromCsvFileIsTheSameAsThatInList(
      String fileName, List<T> dataset, Class<T> type) {
    List<Record> records = getCsvRecords(fileName);
    records.remove(0); // remove header record

    List<Record> notMatch =
        records.stream()
            .filter(
                record -> {
                  boolean found = false;
                  for (T element : dataset) {
                    if (objectHasAllTheValues(record, element, type)) {
                      found = true;
                    }
                  }
                  return !found;
                })
            .collect(Collectors.toList());

    assertTrue(
        notMatch.isEmpty(),
        "The following records were not found in dataset" + notMatch.toString());
  }

  private static <T> boolean objectHasAllTheValues(Record record, T object, Class<T> type) {
    Map<String, String> fieldsMap = record.toFieldMap();
    return fieldsMap.entrySet().stream()
        .allMatch(
            entry -> {
              try {
                Object actual =
                    type.getMethod(
                            "get"
                                + entry.getKey().substring(0, 1).toUpperCase()
                                + entry.getKey().substring(1))
                        .invoke(object);
                return compareValues(actual, entry.getValue());
              } catch (NoSuchMethodException
                  | IllegalAccessException
                  | InvocationTargetException e) {
                return false;
              }
            });
  }

  private static boolean compareValues(Object actual, String expected) {
    // the StringConverter in Sql2o is trimming the strings so we will get the data without the
    // spaces.
    Object expectedAsObject = expected.trim();
    if (actual instanceof BigDecimal) {
      expectedAsObject = new BigDecimal(expected);
      return ((BigDecimal) actual).compareTo((BigDecimal) expectedAsObject) == 0;
    } else if (actual instanceof Date) {
      expectedAsObject = Date.valueOf(expected);
    } else if (actual instanceof Long) {
      expectedAsObject = Long.valueOf(expected);
    } else if (actual instanceof Integer) {
      expectedAsObject = Integer.valueOf(expected);
    } else if (actual instanceof Short) {
      expectedAsObject = Short.valueOf(expected);
    }
    return actual.equals(expectedAsObject);
  }

  public static void readFromCsvFileAndDeleteFromTable(
      SqlDb db, String fileName, String tableName, String keyColumn) {
    List<Record> records = getCsvRecords(fileName);
    records.remove(0); // remove header record

    db.exec(
        "delete " + tableName,
        conn -> {
          records.forEach(
              record -> {
                String deleteStatement =
                    createDeleteStatement(tableName, keyColumn, record.getString(keyColumn));
                try (Query q = conn.createQuery(deleteStatement)) {
                  q.executeUpdate();
                }
              });

          return true;
        });
  }

  private static String createDeleteStatement(String tableName, String column, String value) {
    return "DELETE FROM " + tableName + " WHERE " + column + " = '" + value + "'";
  }

  private static List<Record> getCsvRecords(String fileName) {
    InputStream resourceFileIs = DatabaseCsvUtils.class.getResourceAsStream("/" + fileName);
    CsvParserSettings settings = new CsvParserSettings();
    settings.setLineSeparatorDetectionEnabled(true);
    CsvParser parser = new CsvParser(settings);
    return parser.parseAllRecords(resourceFileIs);
  }

  @Slf4j
  public static class Database {
    private static final Set<String> DB_WHITELIST =
        HashSet.of(
            "cdbr_eds_dosuleanur363_app.worldpaytd.local",
            "cdbm_eds_miud782_app.worldpaytd.local",
            "cdbl_rmb_4_app.worldpaytd.local",
            "cdbl_rmb_5_app.worldpaytd.local");

    private final SqlDb connection;
    private final JdbcConfiguration conf;
    private final String basePath;

    public Database(JdbcConfiguration conf, String basePath) {
      this.connection = SqlDb.simple(conf);
      this.conf = conf;
      this.basePath = basePath;
    }

    private void setup(String... tables) {
      clean(tables);
      Array.of(tables)
          .forEach(
              table ->
                  readFromCsvFileAndWriteToExistingTable(
                      connection, Paths.get(basePath, table + ".csv").toString(), table));
    }

    public void run(Runnable action, String... tables) {
      try {
        setup(tables);
        action.run();
      } finally {
        clean(tables);
      }
      ;
    }

    private void clean(String... tables) {
      DB_WHITELIST
          .find(x -> conf.getUrl().contains(x))
          .peek(x -> log.info("Will truncate tables on {}...", x))
          .getOrElseThrow(
              () ->
                  new RuntimeException(
                      format(
                          "Tried to run TRUNCATE on a non whitelisted DB instance %s.",
                          conf.getUrl())));

      Array.of(tables)
          .forEach(
              table ->
                  connection.exec(
                      format("truncating %s ...", table),
                      c -> c.createQuery(format("DELETE FROM %s", table)).executeUpdate()));
    }
  }
}
