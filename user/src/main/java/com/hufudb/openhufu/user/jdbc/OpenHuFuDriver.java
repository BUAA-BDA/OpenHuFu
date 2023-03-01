package com.hufudb.openhufu.user.jdbc;

import org.apache.calcite.jdbc.Driver;

public class OpenHuFuDriver extends Driver {
  static {
    new OpenHuFuDriver().register();
  }

  public OpenHuFuDriver() {
    super();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:openhufu:";
  }
}
