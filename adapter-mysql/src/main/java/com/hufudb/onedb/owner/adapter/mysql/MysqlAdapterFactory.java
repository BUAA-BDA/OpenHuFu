package com.hufudb.onedb.owner.adapter.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlAdapterFactory implements AdapterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlAdapterFactory.class);

  public MysqlAdapterFactory() {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource.equals("mysql"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new MysqlAdapter(config.catalog, connection, statement, new MysqlTypeConverter());
    } catch (Exception e) {
      LOG.error("Fail to connect to {}: {}", config.url, e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "mysql";
  }
}
