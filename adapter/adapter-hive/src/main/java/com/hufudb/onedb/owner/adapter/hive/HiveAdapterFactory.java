package com.hufudb.onedb.owner.adapter.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.AdapterFactory;

public class HiveAdapterFactory implements AdapterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAdapterFactory.class);

  public HiveAdapterFactory() {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource.equals("hive"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new HiveAdapter(config.catalog, connection, statement, new HiveTypeConverter(), new BasicTranslator(getType()));
    } catch (Exception e) {
      LOG.error("Fail to connect to {}: {}", config.url, e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "hive";
  }
}
