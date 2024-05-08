package com.hufudb.openhufu.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import com.hufudb.openhufu.owner.adapter.AdapterTypeConverter;
import com.hufudb.openhufu.owner.adapter.jdbc.JDBCAdapter;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.EmptyDataSet;
import com.hufudb.openhufu.expression.Translator;
import com.hufudb.openhufu.data.schema.Schema;


public class PostgisAdapter extends JDBCAdapter {
  PostgisAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter, Translator translator) {
    super(catalog, connection, statement, converter, translator);
  }

  @Override
  protected DataSet executeSQL(String sql, Schema schema) {
    try {
      ResultSet rs = statement.executeQuery(sql);
      LOG.info("Execute {}", sql);
      return new PostgisResultDataSet(schema, rs);
    } catch (SQLException e) {
      LOG.error("Fail to execute SQL [{}]: {}", sql, e.getMessage());
      return EmptyDataSet.INSTANCE;
    }
  }
}
