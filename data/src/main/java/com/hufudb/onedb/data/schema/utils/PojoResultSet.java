package com.hufudb.onedb.data.schema.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PojoResultSet {
  public PojoSchema schema;
  public List<List<String>> rows;

  public PojoResultSet() {}

  public PojoResultSet(PojoSchema schema, List<List<String>> rows) {
    this.schema = schema;
    this.rows = rows;
  }

  public static PojoResultSet fromResultSet(ResultSet rs) {
    try {
      ResultSetMetaData meta = rs.getMetaData();
      PojoSchema.Builder builder = PojoSchema.newBuilder();
      List<List<String>> rows = new ArrayList<>();
      int columnCount = meta.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        builder.add(meta.getColumnName(i), TypeConverter.convert2OneDBType(meta.getColumnType(i)));
      }
      while (rs.next()) {
        List<String> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          row.add(rs.getString(i));
        }
        rows.add(row);
      }
      return new PojoResultSet(builder.build(), rows);
    } catch (SQLException e) {
      e.printStackTrace();
      return new PojoResultSet();
    }
  }

  public PojoSchema getSchema() {
    return schema;
  }

  public void setSchema(PojoSchema schema) {
    this.schema = schema;
  }

  public List<List<String>> getRows() {
    return rows;
  }

  public void setRows(List<List<String>> rows) {
    this.rows = rows;
  }
}
