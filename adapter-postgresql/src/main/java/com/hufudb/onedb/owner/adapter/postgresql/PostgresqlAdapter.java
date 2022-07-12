package com.hufudb.onedb.owner.adapter.postgresql;

import java.sql.Connection;
import java.sql.Statement;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;

public class PostgresqlAdapter extends JDBCAdapter {
  PostgresqlAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter) {
    super(catalog, connection, statement, converter, new BasicTranslator());
  }
}
