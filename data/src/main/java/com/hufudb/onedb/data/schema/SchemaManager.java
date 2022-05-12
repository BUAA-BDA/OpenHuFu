package com.hufudb.onedb.data.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for schema mapping, give a virtual table schema for an actual table schema.
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
    actualTableSchemaMap.put(table.getName(), table);
  }

  public void addPublishedTable(PojoPublishedTableSchema table) {
    addPublishedTable(generatePublishedTableSchema(table));
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
    PublishedTableSchema info = publishedTableSchemaMap.get(publishedTableName);
    if (info == null) {
      LOG.error("Not found published table {}", publishedTableName);
      return "";
    } else {
      return info.getActualTableName();
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
    TableSchema actualSchema = actualTableSchemaMap.get(publishedTableSchema.getActualTableName());
    List<PojoColumnDesc> publishedFields = publishedTableSchema.getPublishedColumns();
    List<Integer> originNames = publishedTableSchema.getActualColumns();
    for (int i = 0; i < publishedFields.size(); ++i) {
      if (!publishedFields.get(i).getModifier().equals(Modifier.HIDDEN)) {
        pFields.add(publishedFields.get(i).toColumnDesc());
        mappings.add(originNames.get(i));
      }
    }
    return new PublishedTableSchema(actualSchema, publishedTableSchema.getPublishedTableName(),
        pFields.build(), mappings.build());
  }

  void addPublishedTable(PublishedTableSchema publishedTable) {
    if (publishedTableSchemaMap.containsKey(publishedTable.getPublishedTableName())) {
      LOG.error("published table {} already exist", publishedTable.getPublishedTableName());
    } else {
      publishedTableSchemaMap.put(publishedTable.getPublishedTableName(), publishedTable);
      LOG.info("Add Published Table {}", publishedTable);
    }
  }
}

