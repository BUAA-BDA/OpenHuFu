package com.hufudb.onedb.core.table;

import java.util.ArrayList;
import java.util.List;

public class TableMeta {
  public String tableName;
  public List<LocalTableMeta> localTables;

  public TableMeta() {}

  public TableMeta(String tableName) {
    this.tableName = tableName;
    this.localTables = new ArrayList<>();
  }

  public TableMeta(String tableName, List<LocalTableMeta> feds) {
    this.tableName = tableName;
    this.localTables = feds;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<LocalTableMeta> getFeds() {
    return localTables;
  }

  public void setFeds(List<LocalTableMeta> feds) {
    this.localTables = feds;
  }

  public void addFeds(LocalTableMeta meta) {
    this.localTables.add(meta);
  }

  public void addFeds(String endpoint, String localName) {
    this.localTables.add(new LocalTableMeta(endpoint, localName));
  }

  public static class LocalTableMeta {
    public String endpoint;
    public String localName;

    public LocalTableMeta() {}

    public LocalTableMeta(String endpoint, String localName) {
      this.endpoint = endpoint;
      this.localName = localName;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(String endpoint) {
      this.endpoint = endpoint;
    }

    public String getLocalName() {
      return localName;
    }

    public void setLocalName(String localName) {
      this.localName = localName;
    }

    @Override
    public String toString() {
      return String.format("%s->%s", endpoint, localName);
    }
  }
}
