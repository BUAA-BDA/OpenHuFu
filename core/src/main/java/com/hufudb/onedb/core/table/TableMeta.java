package com.hufudb.onedb.core.table;

import java.util.ArrayList;
import java.util.List;

public class TableMeta {
  public String tableName;
  public List<FedMeta> feds;

  public TableMeta(String tableName) {
    this.tableName = tableName;
    this.feds = new ArrayList<>();
  }

  public TableMeta(String tableName, List<FedMeta> feds) {
    this.tableName = tableName;
    this.feds = feds;
  }

  public void addFeds(FedMeta meta) {
    this.feds.add(meta);
  }

  public void addFeds(String endpoint, String localName) {
    this.feds.add(new FedMeta(endpoint, localName));
  }

  public static class FedMeta {
    public String endpoint;
    public String localName;

    public FedMeta(String endpoint, String localName) {
      this.endpoint = endpoint;
      this.localName = localName;
    }
  }
}
