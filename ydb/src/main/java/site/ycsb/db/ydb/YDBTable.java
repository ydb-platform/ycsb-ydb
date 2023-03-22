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

package site.ycsb.db.ydb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.workloads.CoreWorkload;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.description.ColumnFamily;
import tech.ydb.table.description.StoragePool;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;
import tech.ydb.table.values.PrimitiveType;


/**
 * Helper for create YDB table and keep columns description.
 */
public class YDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(YDBTable.class);

  /** Key column name is 'key' (and type String). */
  private static final String KEY_COLUMN_NAME = "keyColumnName";
  private static final String KEY_COLUMN_NAME_DEFAULT = "id";

  private static final String KEY_DROP_ON_INIT = "dropOnInit";
  private static final String KEY_DROP_ON_INIT_DEFAULT = "false";

  private static final String KEY_DROP_ON_CLEAN = "dropOnClean";
  private static final String KEY_DROP_ON_CLEAN_DEFAULT = "false";

  private static final String KEY_DO_COMPRESSION = "compression";
  private static final String KEY_DO_COMPRESSION_DEFAULT = "false";

  private static final String MAX_PARTITION_SIZE = "2000"; // 2 GB
  private static final String MAX_PARTITIONS_COUNT = "50";

  private final String tableName;
  private final String keyColumnName;
  private final List<String> columnNames;
  private final TableDescription tableDescription;

  private final boolean dropOnInit;
  private final boolean dropOnClean;

  public YDBTable(Properties props) {
    this.tableName = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    this.keyColumnName = props.getProperty(KEY_COLUMN_NAME, KEY_COLUMN_NAME_DEFAULT);

    this.columnNames = new ArrayList<>();
    String fieldPrefix = props.getProperty(CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    int fieldCount = Integer.parseInt(
        props.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT)
    );
    for (int i = 0; i < fieldCount; i++) {
      columnNames.add(fieldPrefix + i);
    }

    this.tableDescription = createTableDescription(props, keyColumnName, columnNames);
    this.dropOnInit = Boolean.parseBoolean(props.getProperty(KEY_DROP_ON_INIT, KEY_DROP_ON_INIT_DEFAULT));
    this.dropOnClean = Boolean.parseBoolean(props.getProperty(KEY_DROP_ON_CLEAN, KEY_DROP_ON_CLEAN_DEFAULT));
  }

  public String name() {
    return this.tableName;
  }

  public String keyColumnName() {
    return this.keyColumnName;
  }

  public List<String> columnNames() {
    return this.columnNames;
  }

  public void init(YDBConnection connection) throws DBException {
    if (!dropOnInit) {
      LOGGER.info("Skip table '{}' creation", tableName);
      return;
    }

    dropTable(connection);

    String tablePath = connection.getDatabase() + "/" + tableName;
    Status createStatus = connection.executeStatus(session -> session.createTable(tablePath, tableDescription)).join();
    if (!createStatus.isSuccess()) {
      String msg = "Failed to create '" + tablePath + "': " + createStatus.toString();
      throw new DBException(msg);
    }

    LOGGER.info("Created table '{}' in database '{}'", tableName, connection.getDatabase());
  }

  public void clean(YDBConnection connection) throws DBException {
    if (!dropOnClean) {
      return;
    }

    dropTable(connection);
  }

  private void dropTable(YDBConnection connection) throws DBException {
    String tablePath = connection.getDatabase() + "/" + tableName;
    Status dropStatus = connection.executeStatus(session -> session.dropTable(tablePath)).join();
    if (!dropStatus.isSuccess() && dropStatus.getCode() != StatusCode.SCHEME_ERROR) {
      String msg = "Failed to drop '" + tablePath + "': " + dropStatus.toString();
      throw new DBException(msg);
    }
  }

  private static TableDescription createTableDescription(
      Properties props, String keyColumnName, List<String> columnNames) {
    boolean doCompression = Boolean.parseBoolean(props.getProperty(KEY_DO_COMPRESSION, KEY_DO_COMPRESSION_DEFAULT));
    boolean autoPartitioning = Boolean.parseBoolean(props.getProperty("autopartitioning", "true"));

    TableDescription.Builder builder = TableDescription.newBuilder();
    String columnFamily = null;

    if (doCompression) {
      columnFamily = "default";
      StoragePool pool = new StoragePool("ssd"); // TODO: must be from opts
      ColumnFamily family = new ColumnFamily(columnFamily, pool, ColumnFamily.Compression.COMPRESSION_LZ4, false);
      builder.addColumnFamily(family);
    }

    builder.addNonnullColumn(keyColumnName, PrimitiveType.Text, columnFamily);
    for (String columnName: columnNames) {
      builder.addNullableColumn(columnName, PrimitiveType.Bytes, columnFamily);
    }
    builder.setPrimaryKey(keyColumnName);

    if (autoPartitioning) {
      int avgRowSize = calculateAvgRowSize(props);
      long recordcount = Long.parseLong(props.getProperty(
          Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

      int maxPartSizeMB = Integer.parseInt(props.getProperty("maxpartsizeMB", MAX_PARTITION_SIZE));
      int maxParts = Integer.parseInt(props.getProperty("maxparts", MAX_PARTITIONS_COUNT));
      long minParts = maxParts;

      long approximateDataSize = avgRowSize * recordcount;
      long avgPartSizeMB = Math.max(approximateDataSize / maxParts / 1000000, 1);
      long partSizeMB = Math.min(avgPartSizeMB, maxPartSizeMB);

      final boolean splitByLoad = Boolean.parseBoolean(props.getProperty("splitByLoad", "true"));
      final boolean splitBySize = Boolean.parseBoolean(props.getProperty("splitBySize", "true"));

      LOGGER.info(String.format(
          "After partitioning for %d records with avg row size %d: " +
          "minParts=%d, maxParts=%d, partSize=%d MB, " +
          "splitByLoad=%b, splitBySize=%b",
          recordcount, avgRowSize, minParts, maxParts, partSizeMB, splitByLoad, splitBySize));

      PartitioningSettings settings = new PartitioningSettings();

      settings.setMinPartitionsCount(minParts);
      settings.setMaxPartitionsCount(maxParts);
      settings.setPartitioningByLoad(splitByLoad);

      if (splitBySize) {
        settings.setPartitionSize(partSizeMB);
        settings.setPartitioningBySize(true);
      } else {
        settings.setPartitioningBySize(true);
      }

      // set both until bug fixed
      builder.setPartitioningSettings(settings);
    }

    return builder.build();
  }

  private static int calculateAvgRowSize(Properties props) {
    int fieldLength = Integer.parseInt(props.getProperty(
        CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));

    int minFieldLength = Integer.parseInt(props.getProperty(
        CoreWorkload.MIN_FIELD_LENGTH_PROPERTY, CoreWorkload.MIN_FIELD_LENGTH_PROPERTY_DEFAULT));

    String fieldLengthDistribution = props.getProperty(
        CoreWorkload.FIELD_LENGTH_DISTRIBUTION_PROPERTY, CoreWorkload.FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    int avgFieldLength;
    if (fieldLengthDistribution.compareTo("constant") == 0) {
      avgFieldLength = fieldLength;
    } else if (fieldLengthDistribution.compareTo("uniform") == 0) {
      if (minFieldLength < fieldLength) {
        avgFieldLength = (fieldLength - minFieldLength) / 2 + 1;
      } else {
        avgFieldLength = fieldLength / 2 + 1;
      }
    } else if (fieldLengthDistribution.compareTo("zipfian") == 0) {
      avgFieldLength = fieldLength / 4 + 1;
    } else if (fieldLengthDistribution.compareTo("histogram") == 0) {
      // TODO: properly handle this case, for now just some value
      avgFieldLength = fieldLength;
    } else {
      avgFieldLength = fieldLength;
    }

    int fieldCount = Integer.parseInt(props.getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    return avgFieldLength * fieldCount;
  }
}