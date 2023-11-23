/*
 * Copyright (c) 2022 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
/**
 * YDB binding for <a href="http://ydb.tech/">YDB</a>.
 *
 * See {@code ydb/README.md} for details.
 */
package site.ycsb.db.ydb;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import tech.ydb.core.Result;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.query.TxMode;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.query.ReadRowsResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB client implementation.
 */
public class YDBClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(YDBClient.class);

  private static boolean usePreparedUpdateInsert = true;
  private static boolean forceUpsert = false;
  private static boolean useQueryService = false;
  private static boolean forceUpdate = false;
  private static boolean useBulkUpsert = false;
  private static boolean useKV = false;
  private static int bulkUpsertBatchSize = 1;

  // all threads must report to this on cleanup
  private static final AtomicLong TOTAL_OKS = new AtomicLong(0);
  private static final AtomicLong TOTAL_ERRORS = new AtomicLong(0);
  private static final AtomicLong TOTAL_NOT_FOUNDS = new AtomicLong(0);

  // per instance counters
  private long oks = 0;
  private long errors = 0;
  private long notFound = 0;

  private Semaphore inflightSemaphore = null;

  private final List<Map<String, Value<?>>> bulkBatch = new ArrayList<>();

  // YDB connection staff
  private YDBConnection connection;

  @Override
  public void init() throws DBException {
    LOGGER.debug("init ydb client");

    Properties properties = getProperties();

    boolean isImport = Boolean.parseBoolean(properties.getProperty("import", "false"));
    if (isImport) {
      properties.setProperty("preparedInsertUpdateQueries", "true");
      properties.setProperty("forceUpsert", "true");
      properties.setProperty("bulkUpsert", "true");
      properties.setProperty("useKV", "false");
      properties.setProperty("useQueryService", "false");
      properties.setProperty("bulkUpsertBatchSize", "1000");

      final boolean presplitTable = Boolean.parseBoolean(
          properties.getProperty(YDBTable.KEY_DO_PRESPLIT, YDBTable.KEY_DO_PRESPLIT_DEFAULT));
      if (presplitTable) {
        properties.setProperty(YDBTable.KEY_DO_PRESPLIT, "true");
      }

      int insertInflight = Integer.parseInt(properties.getProperty("insertInflight", "2"));
      properties.setProperty("insertInflight", String.valueOf(insertInflight));
    }

    connection = YDBConnection.openConnection(getProperties());

    usePreparedUpdateInsert = Boolean.parseBoolean(properties.getProperty("preparedInsertUpdateQueries", "true"));
    forceUpsert = Boolean.parseBoolean(properties.getProperty("forceUpsert", "false"));
    useBulkUpsert = Boolean.parseBoolean(properties.getProperty("bulkUpsert", "false"));
    bulkUpsertBatchSize = Integer.parseInt(properties.getProperty("bulkUpsertBatchSize", "1"));

    useKV = Boolean.parseBoolean(properties.getProperty("useKV", "false"));
    useQueryService = Boolean.parseBoolean(properties.getProperty("useQueryService", "false"));

    forceUpdate = Boolean.parseBoolean(properties.getProperty("forceUpdate", "false"));

    if (connection.inflightSize() > 1) {
      inflightSemaphore = new Semaphore(connection.inflightSize());
    }
  }

  @Override
  public void cleanup() throws DBException {
    LOGGER.debug("cleanup ydb client");

    if (!bulkBatch.isEmpty()) {
      YDBTable table = connection.tables().iterator().next();
      sendBulkBatch(table);
    }

    if (inflightSemaphore != null) {
      try {
        inflightSemaphore.acquire(connection.inflightSize());
        inflightSemaphore.release(connection.inflightSize());
      } catch (InterruptedException e) {
        LOGGER.warn("inflight operations waiting is interrupted", e);
      }
    }

    TOTAL_OKS.addAndGet(oks);
    TOTAL_ERRORS.addAndGet(errors);
    TOTAL_NOT_FOUNDS.addAndGet(notFound);

    if (connection.close()) {
      System.out.println("[TotalOKs] " + TOTAL_OKS);
      System.out.println("[TotalErrors] " + TOTAL_ERRORS);
      System.out.println("[TotalNotFound] " + TOTAL_NOT_FOUNDS);
    }
  }

  private List<ResultSetReader> executeReadByTableService(String query, Params params) {
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    DataQueryResult queryResult = connection.executeResult(
        session -> session.executeDataQuery(query, txControl, params)
    ).join().getValue();

    if (queryResult.getResultSetCount() == 0) {
      return Collections.emptyList();
    }

    return Arrays.asList(queryResult.getResultSet(0));
  }

  private List<ResultSetReader> executeReadByQueryService(String query, Params params) {
    List<ResultSetReader> readers = new ArrayList<>();
    connection.executeQueryStatus(session -> {
        readers.clear();
        return session.executeQuery(query, TxMode.serializableRw(), params).start(part -> {
            readers.add(part.getResultSetReader());
          });
      }).join().expectSuccess("execute query service problem");

    return readers;
  }

  private List<ResultSetReader> executeReadRows(YDBTable table, String key, Set<String> fields) {
    String tablePath = connection.getDatabase() + "/" + table.name();
    List<String> columns = fields == null || fields.isEmpty() ? table.columnNames() : new ArrayList<>(fields);

    ReadRowsSettings settings = ReadRowsSettings.newBuilder()
        .addColumns(columns)
        .addKey(StructValue.of(table.keyColumnName(), PrimitiveValue.newText(key)))
        .build();

    Result<ReadRowsResult> resultWrapped = connection.executeResult(
        session -> session.readRows(tablePath, settings)
    ).join();
    resultWrapped.getStatus().expectSuccess("execute readRows method");
    return Arrays.asList(resultWrapped.getValue().getResultSetReader());
  }

  private List<ResultSetReader> readImpl(YDBTable table, String key, Set<String> fields) {
    if (useKV) {
      return executeReadRows(table, key, fields);
    }

    String fieldsString = "*";
    if (fields != null && !fields.isEmpty()) {
      fieldsString = String.join(",", fields);
    }

    String query = "DECLARE $key as Text; "
        + " SELECT " + fieldsString + " FROM " + table.name()
        + " WHERE " + table.keyColumnName() + " = $key;";

    LOGGER.trace(query);

    Params params = Params.of("$key", PrimitiveValue.newText(key));

    if (useQueryService) {
      return executeReadByQueryService(query, params);
    }

    return executeReadByTableService(query, params);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    LOGGER.debug("read table {} with key {}", table, key);
    YDBTable ydbTable = connection.findTable(table);

    try {
      List<ResultSetReader> list = readImpl(ydbTable, key, fields);

      if (list == null || list.isEmpty() || list.get(0).getRowCount() == 0) {
        ++notFound;
        return Status.NOT_FOUND;
      }

      ResultSetReader rs = list.get(0);
      final int keyColumnIndex = rs.getColumnIndex(ydbTable.keyColumnName());
      while (rs.next()) {
        for (int i = 0; i < rs.getColumnCount(); ++i) {
          if (i == keyColumnIndex) {
            final byte[] val = rs.getColumn(i).getText().getBytes();
            result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          } else {
            final byte[] val = rs.getColumn(i).getBytes();
            result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          }
        }
      }
    } catch (UnexpectedResultException e) {
      LOGGER.error(String.format("Select failed: %s", e.toString()));
      ++errors;
      return Status.ERROR;
    }

    ++oks;
    return Status.OK;
  }

  private List<ResultSetReader> scanRowsByKV(YDBTable table, String startkey, int recordcount, Set<String> fields) {
    String tablePath = connection.getDatabase() + "/" + table.name();
    List<String> columns = fields == null || fields.isEmpty() ? table.columnNames() : new ArrayList<>(fields);
    ReadTableSettings settings = ReadTableSettings.newBuilder()
        .fromKeyInclusive(PrimitiveValue.newText(startkey))
        .rowLimit(recordcount)
        .columns(columns)
        .build();

    List<ResultSetReader> readers = new ArrayList<>();
    connection.executeStatus(session -> {
        readers.clear();
        return session.executeReadTable(tablePath, settings).start(part -> {
            readers.add(part.getResultSetReader());
          });
      }).join().expectSuccess("execute read table problem");

    return readers;
  }

  private List<ResultSetReader> scanImpl(YDBTable table, String startkey, int recordcount, Set<String> fields) {
    if (useKV) {
      return scanRowsByKV(table, startkey, recordcount, fields);
    }

    String fieldsString = "*";
    if (fields != null && !fields.isEmpty()) {
      fieldsString = String.join(",", fields);
    }
    String query = "DECLARE $startKey as Text; DECLARE $limit as Uint32;"
        + " SELECT " + fieldsString + " FROM " + table.name()
        + " WHERE " + table.keyColumnName() + " >= $startKey"
        + " LIMIT $limit;";

    Params params = Params.of(
        "$startKey", PrimitiveValue.newText(startkey),
        "$limit", PrimitiveValue.newUint32(recordcount));

    LOGGER.trace(query);

    if (useQueryService) {
      return executeReadByQueryService(query, params);
    }

    return executeReadByTableService(query, params);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.debug("scan table {} from key {} and size {}", table, startkey, recordcount);
    YDBTable ydbTable = connection.findTable(table);

    try {
      List<ResultSetReader> readers = scanImpl(ydbTable, startkey, recordcount, fields);

      int rowsCount = 0;
      for (ResultSetReader rs: readers) {
        rowsCount += rs.getRowCount();
      }
      result.ensureCapacity(rowsCount);

      for (ResultSetReader rs: readers) {
        final int keyColumnIndex = rs.getColumnIndex(ydbTable.keyColumnName());
        while (rs.next()) {
          HashMap<String, ByteIterator> columns = new HashMap<>();
          for (int i = 0; i < rs.getColumnCount(); ++i) {
            if (i == keyColumnIndex) {
              final byte[] val = rs.getColumn(i).getText().getBytes();
              columns.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
            } else {
              final byte[] val = rs.getColumn(i).getBytes();
              columns.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
            }
          }
          result.add(columns);
        }
      }
    } catch (UnexpectedResultException e) {
      LOGGER.error(String.format("Scan failed: %s", e.toString()));
      ++errors;
      return Status.ERROR;
    }

    ++oks;
    return Status.OK;
  }

  private CompletableFuture<tech.ydb.core.Status> executeUpdateByTableService(String query, Params params) {
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);
    return connection.executeStatus(
        session -> session.executeDataQuery(query, txControl, params).thenApply(Result::getStatus)
    );
  }

  private CompletableFuture<tech.ydb.core.Status> executeUpdateByQueryService(String query, Params params) {
    return connection.executeQueryStatus(
        session -> session.executeQuery(query, TxMode.serializableRw(), params).start(part -> {})
    );
  }

  private CompletableFuture<tech.ydb.core.Status> executeQueryImpl(String query, Params params) {
    if (useQueryService) {
      return executeUpdateByQueryService(query, params);
    }

    return executeUpdateByTableService(query, params);
  }

  private Status executeQuery(String query, Params params, String op) {
    LOGGER.trace(query);

    try {
      if (inflightSemaphore != null) {
        inflightSemaphore.acquire();
        executeQueryImpl(query, params).whenComplete((result, th) -> {
            if (th == null && result != null && result.isSuccess()) {
              ++oks;
            } else {
              ++errors;
            }
            inflightSemaphore.release();
          });
      } else {
        executeQueryImpl(query, params).join().expectSuccess(String.format("execute %s query problem", op));
        ++oks;
      }

      return Status.OK;
    } catch (InterruptedException | RuntimeException e) {
      LOGGER.error(e.toString());
      ++errors;
      return Status.ERROR;
    }
  }

  private Status updatePrepared(String table, String key, Map<String, ByteIterator> values) {
    YDBTable ydbTable = connection.findTable(table);

    final StringBuilder queryDeclare = new StringBuilder();
    final Params params = Params.create();

    queryDeclare.append("DECLARE $key AS Text;");
    params.put("$key", PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        queryDeclare.append("DECLARE $").append(column).append(" AS Bytes;");
        params.put("$" + column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    String valuesString = values.entrySet().stream()
        .map(e -> e.getKey() + "=$" + e.getKey())
        .collect(Collectors.joining(","));

    String query = queryDeclare.toString() + " UPDATE " + ydbTable.name() + " SET "
        + valuesString + " WHERE " + ydbTable.keyColumnName() + "=$key;";

    return executeQuery(query, params, "update");
  }

  private Status insertOrUpsertPrepared(String table, String key, Map<String, ByteIterator> values, String op) {
    YDBTable ydbTable = connection.findTable(table);

    final StringBuilder queryDeclare = new StringBuilder();
    final StringBuilder queryColumns = new StringBuilder();
    final StringBuilder queryParams = new StringBuilder();
    final Params params = Params.create();

    queryDeclare.append("DECLARE $key AS Text;");
    queryColumns.append(ydbTable.keyColumnName());
    queryParams.append("$key");
    params.put("$key", PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        queryDeclare.append("DECLARE $").append(column).append(" AS Bytes;");
        queryColumns.append(", ").append(column);
        queryParams.append(", $").append(column);
        params.put("$" + column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    String query = queryDeclare.toString() + op + " INTO " + ydbTable.name()
        + " (" + queryColumns.toString() + " ) VALUES ( " + queryParams.toString() + ");";

    return executeQuery(query, params, op);
  }

  private Status insertOrUpsertNotPrepared(String table, String key, Map<String, ByteIterator> values, String op) {
    YDBTable ydbTable = connection.findTable(table);

    final StringBuilder queryColumns = new StringBuilder();
    final StringBuilder queryValues = new StringBuilder();

    queryColumns.append(ydbTable.keyColumnName());
    queryValues.append("'").append(key).append("'");

    values.forEach((column, bytes) -> {
        queryColumns.append(", ").append(column);
        queryValues.append("'").append(bytes.toArray()).append("'");
      });

    String query = op + " INTO " + ydbTable.name()
        + " (" + queryColumns.toString() + " ) VALUES ( " + queryValues.toString() + ");";

    return executeQuery(query, Params.empty(), op);
  }

  private Status sendBulkBatch(YDBTable ydbTable) {
    if (bulkBatch.isEmpty()) {
      return Status.OK;
    }

    int bulkSize = bulkBatch.size();
    String tablePath = connection.getDatabase() + "/" + ydbTable.name();

    final Map<String, Type> ydbTypes = new HashMap<>();
    ydbTypes.put(ydbTable.keyColumnName(), PrimitiveType.Text);

    OptionalType optionalBytes = OptionalType.of(PrimitiveType.Bytes);
    ydbTable.columnNames().forEach(column -> {
        ydbTypes.put(column, optionalBytes);
      });

    StructType type = StructType.of(ydbTypes);

    try {
      ListValue bulkData = ListType.of(type).newValue(
          bulkBatch.stream().map(type::newValue).collect(Collectors.toList())
      );
      bulkBatch.clear();

      if (inflightSemaphore != null) {
        inflightSemaphore.acquire();
      }

      CompletableFuture<tech.ydb.core.Status> future = connection
          .executeStatus(session -> session.executeBulkUpsert(tablePath, bulkData));

      if (inflightSemaphore != null) {
        future.whenComplete((result, th) -> {
            if (th == null && result != null && result.isSuccess()) {
              oks += bulkSize;
            } else {
              errors += bulkSize;
            }
            inflightSemaphore.release();
          });
      } else {
        future.join().expectSuccess("bulk upsert problem for bulk size " + bulkSize);
        oks += bulkSize;
      }

      return Status.OK;
    } catch (InterruptedException | RuntimeException e) {
      LOGGER.error(e.toString());
      errors += bulkSize;
      return Status.ERROR;
    }
  }

  private Status bulkUpsertBatched(YDBTable ydbTable, String key, Map<String, ByteIterator> values) {
    Map<String, Value<?>> ydbValues = new HashMap<>();
    ydbValues.put(ydbTable.keyColumnName(), PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        ydbValues.put(column, OptionalValue.of(PrimitiveValue.newBytes(bytes.toArray())));
      });

    OptionalValue emptyBytes = OptionalType.of(PrimitiveType.Bytes).emptyValue();
    ydbTable.columnNames().forEach(column -> {
        if (!ydbValues.containsKey(column)) {
          ydbValues.put(column, emptyBytes);
        }
      });


    bulkBatch.add(ydbValues);
    if (bulkBatch.size() < bulkUpsertBatchSize) {
      return Status.BATCHED_OK;
    }

    return sendBulkBatch(ydbTable);
  }

  private Status bulkUpsert(String table, String key, Map<String, ByteIterator> values) {
    YDBTable ydbTable = connection.findTable(table);
    String tablePath = connection.getDatabase() + "/" + ydbTable.name();

    if (bulkUpsertBatchSize > 1) {
      return bulkUpsertBatched(ydbTable, key, values);
    }

    final Map<String, Type> ydbTypes = new HashMap<>();
    final Map<String, Value<?>> ydbValues = new HashMap<>();

    ydbTypes.put(ydbTable.keyColumnName(), PrimitiveType.Text);
    ydbValues.put(ydbTable.keyColumnName(), PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        ydbTypes.put(column, PrimitiveType.Bytes);
        ydbValues.put(column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    StructValue data = StructType.of(ydbTypes).newValue(ydbValues);

    try {
      connection.executeStatus(session -> session.executeBulkUpsert(tablePath, ListValue.of(data)))
          .join().expectSuccess("bulk upsert problem for key " + key);
      ++oks;
      return Status.OK;
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      ++errors;
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    LOGGER.debug("update record table {} with key {}", table, key);

    if (useKV) { // KeyValue service always uses BulkUpsert
      return bulkUpsert(table, key, values);
    }

    if (usePreparedUpdateInsert) {
      if (forceUpdate) {
        return updatePrepared(table, key, values);
      }

      // note that is is a blind update: i.e. we will never return NOT_FOUND
      if (useBulkUpsert) {
        return bulkUpsert(table, key, values);
      } else {
        return insertOrUpsertPrepared(table, key, values, "UPSERT");
      }
    } else {
      return insertOrUpsertNotPrepared(table, key, values, "UPSERT");
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    LOGGER.debug("insert record into table {} with key {}", table, key);
    // note that inserting same key twice results into error
    if (forceUpsert || useKV) {
      return update(table, key, values);
    }

    if (usePreparedUpdateInsert) {
      return insertOrUpsertPrepared(table, key, values, "INSERT");
    } else {
      return insertOrUpsertNotPrepared(table, key, values, "INSERT");
    }
  }

  @Override
  public Status delete(String table, String key) {
    LOGGER.debug("delete record from table {} with key {}", table, key);
    YDBTable ydbTable = connection.findTable(table);

    String query = "DECLARE $key as Text; "
        + "DELETE from " + ydbTable.name()
        + " WHERE " + ydbTable.keyColumnName() + " = $key;";
    LOGGER.debug(query);

    Params params = Params.of("$key", PrimitiveValue.newText(key));

    try {
      StatusCode code = executeQueryImpl(query, params).join().getCode();

      switch (code) {
      case SUCCESS:
        ++oks;
        return Status.OK;
      case NOT_FOUND:
        ++notFound;
        return Status.NOT_FOUND;
      default:
        ++errors;
        return Status.ERROR;
      }
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      return Status.ERROR;
    }
  }
}
