package com.hufudb.onedb.owner.adapter.kylin;

import com.hufudb.onedb.expression.Translator;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;
import java.sql.Connection;
import java.sql.Statement;

public class KylinAdapter extends JDBCAdapter {
  KylinAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter, Translator translator) {
    super(catalog, connection, statement, converter, translator);
  }
}
