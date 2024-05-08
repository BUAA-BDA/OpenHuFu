package com.hufudb.openhufu.owner.adapter.postgis;

import com.hufudb.openhufu.common.enums.DataSourceType;
import com.hufudb.openhufu.owner.adapter.AdapterConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.hufudb.openhufu.owner.adapter.AdapterFactory;
import com.hufudb.openhufu.expression.BasicTranslator;
import com.hufudb.openhufu.owner.adapter.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgisAdapterFactory implements AdapterFactory {

  static final Logger LOG = LoggerFactory.getLogger(PostgisAdapterFactory.class);

  public PostgisAdapterFactory() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter create(AdapterConfig config) {
    try {
      Connection connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      Statement statement = connection.createStatement();
      return new PostgisAdapter(config.catalog, connection, statement, new PostgisTypeConverter(),
          new BasicTranslator(getType().getType()));
    } catch (Exception e) {
      LOG.error("Fail to create csv adapter: {}", config.url, e);
      return null;
    }
  }

  @Override
  public DataSourceType getType() {
    return DataSourceType.POSTGIS;
  }
}
