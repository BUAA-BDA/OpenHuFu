package com.hufudb.onedb.core.table;

import java.util.ArrayList;
import java.util.List;

public class GlobalTableConfig {
  public String tableName;
  public List<LocalTableConfig> localTables;

  public GlobalTableConfig() {}

  public GlobalTableConfig(String tableName) {
    this.tableName = tableName;
    this.localTables = new ArrayList<>();
  }

  public GlobalTableConfig(String tableName, List<LocalTableConfig> feds) {
    this.tableName = tableName;
    this.localTables = feds;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<LocalTableConfig> getLocalTables() {
    return localTables;
  }

  public void addLocalTables(List<LocalTableConfig> feds) {
    this.localTables = feds;
  }

  public void addLocalTable(LocalTableConfig meta) {
    this.localTables.add(meta);
  }

  public void addLocalTable(String endpoint, String localName) {
    this.localTables.add(new LocalTableConfig(endpoint, localName));
  }
}
