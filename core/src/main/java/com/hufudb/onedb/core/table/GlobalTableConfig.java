package com.hufudb.onedb.core.table;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.SerializedName;

public class GlobalTableConfig {
  @SerializedName(value = "tableName", alternate = {"tablename", "table_name"})
  public String tableName;
  @SerializedName(value = "localTables", alternate = {"localtables", "local_tables"})
  public List<LocalTableConfig> localTables;

  public GlobalTableConfig() {}

  public GlobalTableConfig(String tableName) {
    this.tableName = tableName;
    this.localTables = new ArrayList<>();
  }

  public GlobalTableConfig(String tableName, List<LocalTableConfig> localTables) {
    this.tableName = tableName;
    this.localTables = localTables;
  }
}
