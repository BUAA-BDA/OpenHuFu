package com.hufudb.onedb.core.table;

public class LocalTableConfig {
  public String endpoint;
  public String localName;

  public LocalTableConfig() {}

  public LocalTableConfig(String endpoint, String localName) {
    this.endpoint = endpoint;
    this.localName = localName;
  }

  @Override
  public String toString() {
    return String.format("%s->%s", endpoint, localName);
  }
}