package com.hufudb.onedb.core.data.utils;

import com.hufudb.onedb.core.data.TypeConverter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class POJOResultSet {
  POJOHeader header;
  List<List<String>> rows;

  public POJOResultSet() {}

  public POJOResultSet(POJOHeader header, List<List<String>> rows) {
    this.header = header;
    this.rows = rows;
  }

  public POJOHeader getHeader() {
    return header;
  }

  public void setHeader(POJOHeader header) {
    this.header = header;
  }

  public List<List<String>> getRows() {
    return rows;
  }

  public void setRows(List<List<String>> rows) {
    this.rows = rows;
  }

  public static POJOResultSet fromResultSet(ResultSet rs) {
    try {
      ResultSetMetaData meta = rs.getMetaData();
      POJOHeader.Builder builder = POJOHeader.newBuilder();
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
      return new POJOResultSet(builder.build(), rows);
    } catch (SQLException e) {
      e.printStackTrace();
      return new POJOResultSet();
    }
  }
}
