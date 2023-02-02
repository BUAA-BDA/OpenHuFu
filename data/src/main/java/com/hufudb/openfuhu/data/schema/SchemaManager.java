package com.hufudb.openhufu.data.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.utils.PojoColumnDesc;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnDesc;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for owner side schema mapping, give a virtual table schema for an actual table schema.
 * The virtual schema could has different table name, column names, column modifier from actual table,
 * the order of the columns can also be changed.
 */
public class SchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

  final Map<String, TableSchema> actualTableSchemaMap;
  final Map<String, PublishedTableSchema> publishedTableSchemaMap;

  public SchemaManager() {
    actualTableSchemaMap = new ConcurrentHashMap<>();
    publishedTableSchemaMap = new ConcurrentHashMap<>();
  }

  public void addLocalTable(TableSchema table) {
    if (actualTableSchemaMap.containsKey(table.getName())) {
      LOG.error("Local table {} already existed", table.getName());
      return;
    }
    actualTableSchemaMap.put(table.getName(), table);
    LOG.info("Found local table {}", table);
  }

  public boolean addPublishedTable(PojoPublishedTableSchema table) {
    return addPublishedTable(generatePublishedTableSchema(table));
  }

  public TableSchema getLocalTable(String tableName) {
    return actualTableSchemaMap.get(tableName);
  }

  public List<TableSchema> getAllLocalTable() {
    ImmutableList.Builder<TableSchema> tables = ImmutableList.builder();
    for (TableSchema table : actualTableSchemaMap.values()) {
      tables.add(table);
    }
    return tables.build();
  }

  public String getActualTableName(String publishedTableName) {
    PublishedTableSchema schema = publishedTableSchemaMap.get(publishedTableName);
    if (schema == null) {
      LOG.error("Not found published table {}", publishedTableName);
      return "";
    } else {
      return schema.getActualTableName();
    }
  }

  public Schema getPublishedSchema(String publishedTableName) {
    PublishedTableSchema schema = publishedTableSchemaMap.get(publishedTableName);
    if (schema == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return Schema.EMPTY;
    } else {
      return schema.getFakeTableSchema().getSchema();
    }
  }

  public Schema getActualSchema(String publishedTableName) {
    PublishedTableSchema schema = publishedTableSchemaMap.get(publishedTableName);
    if (schema == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return Schema.EMPTY;
    } else {
      return schema.getActualSchema();
    }
  }

  public List<Integer> getPublishedSchemaMapping(String publishedTableName) {
    PublishedTableSchema schema = publishedTableSchemaMap.get(publishedTableName);
    if (schema == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return ImmutableList.of();
    } else {
      return schema.getMappings();
    }
  }

  public void clearPublishedTable() {
    publishedTableSchemaMap.clear();
  }

  public void dropPublishedTable(String tableName) {
    publishedTableSchemaMap.remove(tableName);
  }

  public List<PublishedTableSchema> getAllPublishedTable() {
    ImmutableList.Builder<PublishedTableSchema> tables = ImmutableList.builder();
    for (PublishedTableSchema table : publishedTableSchemaMap.values()) {
      tables.add(table);
    }
    return tables.build();
  }

  PublishedTableSchema generatePublishedTableSchema(PojoPublishedTableSchema publishedTableSchema) {
    ImmutableList.Builder<ColumnDesc> pFields = ImmutableList.builder();
    ImmutableList.Builder<Integer> mappings = ImmutableList.builder();
    TableSchema actualSchema = actualTableSchemaMap.get(publishedTableSchema.getActualName());
    if (actualSchema == null) {
      LOG.error("Table {} not found in local database", publishedTableSchema.getActualName());
      throw new RuntimeException("Table not found in local database");
    }
    List<PojoColumnDesc> publishedColumns = publishedTableSchema.getPublishedColumns();
    for (int i = 0; i < publishedColumns.size(); ++i) {
      if (!publishedColumns.get(i).getModifier().equals(Modifier.HIDDEN)) {
        pFields.add(publishedColumns.get(i).toColumnDesc());
        mappings.add(publishedColumns.get(i).getColumnId());
      }
    }
    return new PublishedTableSchema(actualSchema, publishedTableSchema.getPublishedName(),
        pFields.build(), mappings.build());
  }

  boolean addPublishedTable(PublishedTableSchema publishedTable) {
    if (publishedTableSchemaMap.containsKey(publishedTable.getPublishedTableName())) {
      LOG.error("published table {} already exist", publishedTable.getPublishedTableName());
      return false;
    } else {
      publishedTableSchemaMap.put(publishedTable.getPublishedTableName(), publishedTable);
      LOG.info("Add Published Table {}", publishedTable);
      return true;
    }
  }
}

