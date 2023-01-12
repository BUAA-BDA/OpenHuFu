package com.hufudb.onedb.owner.adapter.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class OracleAdapterFactory implements AdapterFactory {

  public OracleAdapterFactory() {
    try {
      Class.forName("oracle.jdbc.OracleDriver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert (config.datasource.equals("oracle"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new OracleAdapter(config.catalog, connection, statement, new OracleTypeConverter(),
          new BasicTranslator(getType()));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "oracle";
  }
}
