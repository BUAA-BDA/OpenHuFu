package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.schema.Schema;


public class PostgisAdapter extends JDBCAdapter {
  PostgisAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter) {
    super(catalog, connection, statement, converter, new PostgisTranslator());
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
