package com.hufudb.onedb.data.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.utils.PojoActualTableSchema;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoDesensitize;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.proto.OneDBData.Desensitize;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for owner side schema mapping, give a virtual table schema for an actual table schema.
 * The virtual schema could has different table name, column names, column modifier from actual table,
 * the order of the columns can also be changed.
 */
public class SchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

  public final Map<String, TableSchema> actualTableSchemaMap;
  final Map<String, PublishedTableSchema> publishedTableSchemaMap;
  public Map<String, TableSchema> desensitizationMap;

  public SchemaManager() {
    actualTableSchemaMap = new ConcurrentHashMap<>();
    publishedTableSchemaMap = new ConcurrentHashMap<>();
    desensitizationMap = new ConcurrentHashMap<>();
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

  public void initDesensitization(List<PojoActualTableSchema> actualTables) {
    if (actualTables != null) {
      for (PojoActualTableSchema actualTable : actualTables) {
        String name = actualTable.getActualName();
        List<ColumnDesc> columnDescs = new ArrayList<>();
        for (PojoColumnDesc columnDesc : actualTable.getActualColumns()) {
          columnDescs.add(columnDesc.DtoColumnDesc());
        }
        TableSchema schema = TableSchema.of(name, columnDescs);
        if (!desensitizationMap.containsKey(name)) {
          desensitizationMap.put(name, schema);
        }
      }
    }
    addLocalTableSensitivity();
    desensitizationMap = actualTableSchemaMap;
  }

  public void addLocalTableSensitivity() {
    for (String table : this.actualTableSchemaMap.keySet()) {
      TableSchema actualTable = actualTableSchemaMap.get(table);
      if (desensitizationMap.containsKey(table)) {
        TableSchema desensitizeTable = desensitizationMap.get(table);
        List<ColumnDesc> deColumnDescs = new ArrayList<>();
        for (ColumnDesc columnDesc : actualTable.getSchema().getColumnDescs()) {
          ColumnDesc tmp = columnDesc;
          String colName = columnDesc.getName();
          ColumnType type = columnDesc.getType();
          Modifier modifier = columnDesc.getModifier();
          Desensitize desensitize = desensitizeTable.getDesensitize(colName);
          if (desensitize != null) {
            tmp = ColumnDesc.newBuilder().setName(colName).setType(type).setModifier(modifier).setDesensitize(desensitize).build();
          }
          deColumnDescs.add(tmp);
          actualTableSchemaMap.remove(table);
          actualTableSchemaMap.put(table, TableSchema.of(table, deColumnDescs));
        }
      }
    }
  }

  public Map<String, TableSchema> getDesensitizationMap() {
    return actualTableSchemaMap;
  }

  public void updateDesensitize(String tableName, PojoColumnDesc columnDesc) {
    try {
      TableSchema actualTable = this.getLocalTable(tableName);
      actualTableSchemaMap.remove(tableName, actualTableSchemaMap.get(tableName));
      String name = columnDesc.name;
      PojoDesensitize desensitize = columnDesc.desensitize;
      List<ColumnDesc> columnDescs = new ArrayList<>();
      for (int index = 0; index < actualTable.schema.getColumnDescs().size(); index++) {
        if (index != actualTable.getColumnIndex(name)) {
          columnDescs.add(actualTable.schema.getColumnDesc(index));
        } else {
          ColumnDesc originCol = actualTable.schema.getColumnDesc(index);
          columnDescs.add(ColumnDesc.newBuilder().setName(name).setType(originCol.getType()).setDesensitize(desensitize.toDesensitize()).build());
        }
      }
      actualTable = TableSchema.of(tableName, columnDescs);
      actualTableSchemaMap.put(tableName, actualTable);
    } catch (Exception e) {
      LOG.error(String.valueOf(e));
    }
  }


}

