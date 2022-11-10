package com.hufudb.onedb.owner.adapter.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class SqliteAdapterFactory implements AdapterFactory {

  public SqliteAdapterFactory() {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert (config.datasource.equals("sqlite"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new SqliteAdapter(config.catalog, connection, statement, new SqliteTypeConverter(),
          new BasicTranslator(getType()));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "sqlite";
  }
}
