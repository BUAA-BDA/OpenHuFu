package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class PostgisAdapterFactory implements AdapterFactory {

  public PostgisAdapterFactory() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert (config.datasource.equals("postgis"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new PostgisAdapter(config.catalog, connection, statement, new PostgisTypeConverter(),
          new BasicTranslator(getType()));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "postgis";
  }
}
