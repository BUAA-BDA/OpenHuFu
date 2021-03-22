package tk.onedb.client.jdbc;

import org.apache.calcite.jdbc.Driver;

public class OneDBDriver extends Driver {
  static {
    new OneDBDriver().register();
  }

  public OneDBDriver() {
    super();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:onedb:";
  }
}
