package com.hufudb.onedb.backend.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.TypeConverter;


public class SimpleResultSet {
  Header header;
  List<List<String>> rows;

  public SimpleResultSet() {}

  public SimpleResultSet(Header header, List<List<String>> rows) {
    this.header = header;
    this.rows = rows;
  }

  public Header getHeader() {
    return header;
  }
  public void setHeader(Header header) {
    this.header = header;
  }
  public List<List<String>> getRows() {
    return rows;
  }
  public void setRows(List<List<String>> rows) {
    this.rows = rows;
  }

  public static SimpleResultSet fromResultSet(ResultSet rs) {
    try {
      ResultSetMetaData meta = rs.getMetaData();
      Header.Builder builder = Header.newBuilder();
      List<List<String>> rows = new ArrayList<>();
      int columnCount = meta.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        builder.add(meta.getColumnName(i), TypeConverter.convert2OneDBTyep(meta.getColumnType(i)));
      }
      while (rs.next()) {
        List<String> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          row.add(rs.getString(i));
        }
        rows.add(row);
      }
      return new SimpleResultSet(builder.build(), rows);
    } catch (SQLException e) {
      e.printStackTrace();
      return new SimpleResultSet();
    }
  }
}
