package com.hufudb.openhufu.core.config.wyx_task;

public class WXY_InputDataItem {
  private String domainID;
  private String role;
  private Params params;

  public String getDomainID() {
    return domainID;
  }

  public String getRole() {
    return role;
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
