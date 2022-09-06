package com.hufudb.onedb.owner.adapter.hive;

import java.sql.*;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Before testing, start docker in docker/hereto/hive
 */
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

      //test metadata, expected result see init.sql
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

      //test query
      String sql = "select * from student";
      System.out.println("Running: " + sql);
      ResultSet res = stmt.executeQuery(sql);
      int count = 0;
      while (res.next()) {
        System.out.println(String.valueOf(res.getString(1)));
        count++;
      }
      assertEquals(3, count);
      res.close();

      sql = "select * from student where score < 88.5";
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      res.next();
      assertEquals("helen", res.getString(1));
      assertEquals(75.4, res.getDouble(3), 0.001);
      assertFalse(res.next());
      res.close();

      sql = "select SUM(age) from student";
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      res.next();
      assertEquals(66, res.getInt(1));
      assertFalse(res.next());
      res.close();

      sql = "select * from time";
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      res.next();
      assertEquals(1, res.getInt(1));
      assertEquals(Date.valueOf("2018-06-01"), res.getDate(2));
      assertEquals(Timestamp.valueOf("2018-06-01 10:14:45"), res.getTimestamp(3));
      count = 1;
      while (res.next()) {
        count ++;
        System.out.println(String.valueOf(res.getDate(2)));
      }
      assertEquals(6, count);
      res.close();

      sql = "select id from time where test_date > '2019-06-01' and test_timestamp < '2020-05-01 05:15:55'";
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      res.next();
      assertEquals(3, res.getInt(1));
      res.next();
      assertEquals(4, res.getInt(1));
      res.next();
      assertFalse(res.next());
      res.close();

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}