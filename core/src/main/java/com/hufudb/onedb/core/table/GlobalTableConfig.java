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
}
