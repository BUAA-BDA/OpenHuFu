package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;

public class PostgisAdapter extends JDBCAdapter {
  PostgisAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter) {
    super(catalog, connection, statement, converter);
  }
}
