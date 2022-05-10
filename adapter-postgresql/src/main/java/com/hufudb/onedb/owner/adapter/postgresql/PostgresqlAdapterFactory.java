package com.hufudb.onedb.owner.adapter.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class PostgresqlAdapterFactory implements AdapterFactory {

  public PostgresqlAdapterFactory() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource.equals("postgresql"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new PostgresqlAdapter(config.catalog, connection, statement, new PostgresqlTypeConverter());
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "postgresql";
  }
}
