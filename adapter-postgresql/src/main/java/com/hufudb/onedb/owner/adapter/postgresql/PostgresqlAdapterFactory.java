package com.hufudb.onedb.owner.adapter.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class PostgresqlAdapterFactory implements AdapterFactory {
  final AdapterConfig config;

  public PostgresqlAdapterFactory() {
    config = new AdapterConfig();
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public PostgresqlAdapterFactory setHostname(String hostname) {
    config.hostname = hostname;
    return this;
  }

  public PostgresqlAdapterFactory setCatalog(String catalog) {
    config.catalog = catalog;
    return this;
  }

  public PostgresqlAdapterFactory setUrl(String url) {
    config.url = url;
    return this;
  }

  public PostgresqlAdapterFactory setUser(String user) {
    config.user = user;
    return this;
  }

  public PostgresqlAdapterFactory setPasswd(String passwd) {
    config.passwd = passwd;
    return this;
  }

  public Adapter build() {
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
  public Adapter create(AdapterConfig config) {
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
