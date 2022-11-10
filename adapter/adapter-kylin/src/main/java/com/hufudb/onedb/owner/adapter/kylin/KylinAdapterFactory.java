package com.hufudb.onedb.owner.adapter.kylin;

import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinAdapterFactory implements AdapterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KylinAdapterFactory.class);

  public KylinAdapterFactory() {
    try {
      Class.forName("org.apache.kylin.jdbc.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource.equals("kylin"));
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new KylinAdapter(config.catalog, connection, statement, new KylinTypeConverter(), new BasicTranslator(getType()));
    } catch (Exception e) {
      LOG.error("Fail to connect to {}: {}", config.url, e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "kylin";
  }
}
