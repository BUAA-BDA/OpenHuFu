package com.hufudb.onedb.owner.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.PublishedTableInfo;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

  final Map<String, TableInfo> localTableInfoMap;
  final Map<String, PublishedTableInfo> publishedTableInfoMap;

  public SchemaManager() {
    localTableInfoMap = new ConcurrentHashMap<>();
    publishedTableInfoMap = new ConcurrentHashMap<>();
  }

  public void addLocalTable(TableInfo table) {
    localTableInfoMap.put(table.getName(), table);
  }

  public void addPublishedTable(POJOPublishedTableInfo table) {
    addPublishedTable(generatePublishedTableInfo(table));
  }

  public TableInfo getLocalTable(String tableName) {
    return localTableInfoMap.get(tableName);
  }

  public List<TableInfo> getAllLocalTable() {
    ImmutableList.Builder<TableInfo> tables = ImmutableList.builder();
    for (TableInfo table : localTableInfoMap.values()) {
      tables.add(table);
    }
    return tables.build();
  }

  public String getLocalTableName(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      LOG.error("Not found published table {}", publishedTableName);
      return "";
    } else {
      return info.getOriginTableName();
    }
  }

  public Header getPublishedTableHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return Header.EMPTY;
    } else {
      return info.getFakeHeader();
    }
  }

  public void clearPublishedTable() {
    publishedTableInfoMap.clear();
  }

  public void dropPublishedTable(String tableName) {
    publishedTableInfoMap.remove(tableName);
  }

  public List<PublishedTableInfo> getAllPublishedTable() {
    ImmutableList.Builder<PublishedTableInfo> tables = ImmutableList.builder();
    for (PublishedTableInfo table : publishedTableInfoMap.values()) {
      tables.add(table);
    }
    return tables.build();
  }

  PublishedTableInfo generatePublishedTableInfo(POJOPublishedTableInfo publishedTableInfo) {
    ImmutableList.Builder<Field> pFields = ImmutableList.builder();
    ImmutableList.Builder<Integer> mappings = ImmutableList.builder();
    TableInfo originInfo = localTableInfoMap.get(publishedTableInfo.getOriginTableName());
    List<Field> publishedFields = publishedTableInfo.getPublishedFields();
    List<Integer> originNames = publishedTableInfo.getOriginColumns();
    for (int i = 0; i < publishedFields.size(); ++i) {
      if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
        pFields.add(publishedFields.get(i));
        mappings.add(originNames.get(i));
      }
    }
    return new PublishedTableInfo(originInfo, publishedTableInfo.getPublishedTableName(),
        pFields.build(), mappings.build());
  }

  void addPublishedTable(PublishedTableInfo publishedTable) {
    if (publishedTableInfoMap.containsKey(publishedTable.getPublishedTableName())) {
      LOG.error("published table {} already exist", publishedTable.getPublishedTableName());
    } else {
      publishedTableInfoMap.put(publishedTable.getPublishedTableName(), publishedTable);
      LOG.info("Add Published Table {}", publishedTable);
    }
  }
}
