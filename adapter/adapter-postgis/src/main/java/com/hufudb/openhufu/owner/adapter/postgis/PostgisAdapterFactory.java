package com.hufudb.openhufu.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.hufudb.openhufu.common.enums.DataSourceType;
import com.hufudb.openhufu.owner.adapter.AdapterFactory;
import com.hufudb.openhufu.expression.BasicTranslator;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.adapter.AdapterConfig;

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
    assert (config.datasource.equals(DataSourceType.POSTGIS));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new PostgisAdapter(config.catalog, connection, statement, new PostgisTypeConverter(),
          new BasicTranslator(getType().getType().toLowerCase()));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public DataSourceType getType() {
    return DataSourceType.POSTGIS;
  }
}
