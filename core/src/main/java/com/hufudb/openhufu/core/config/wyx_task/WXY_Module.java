package com.hufudb.openhufu.core.config.wyx_task;

public class WXY_Module {
  private String moduleName;
  private Params params;

  public String getModuleName() {
    return moduleName;
  }

  public String getPoint() {
    return params.point;
  }

  public int getRange() {
    return params.range;
  }

  public int  getK() {
    return params.k;
  }

  private class Params {
    public String point;
    public int range;
    public int k;
  }
}
