package com.hufudb.onedb.owner.adapter.hive;

import java.sql.*;

import com.hufudb.onedb.data.schema.TableSchema;
import org.junit.Ignore;
import org.junit.Test;

public class HiveAdapterTest {

  @Ignore
  @Test
  public void testHive() {
    String driverName = "org.apache.hive.jdbc.HiveDriver";

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    try{
      Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      Statement stmt = con.createStatement();

      String sql = "select * from student";
      System.out.println("Running: " + sql);
      ResultSet res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(String.valueOf(res.getString(1)));
      }

      DatabaseMetaData meta = con.getMetaData();
      ResultSet rs = meta.getTables("default", null, "%", new String[] {"TABLE"});
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        System.out.println(tableName);
        ResultSet rc = meta.getColumns("default", null, tableName, null);
        while (rc.next()) {
          String columnName = rc.getString("COLUMN_NAME");
          String typename = rc.getString("TYPE_NAME");
          System.out.println(columnName + " " + typename);
        }
        rc.close();
      }
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
