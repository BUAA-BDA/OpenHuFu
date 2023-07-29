package com.hufudb.openhufu.core.config.wyx_task;

public class WXY_DataItem {
  private String domainID;
  private Params params;

  public String getDomainID() {
    return domainID;
  }

  public String getTable() {
    return params.table;
  }

  public String getField() {
    return params.field;
  }

  private class Params {
    public String table;
    public String field;
  }
}
