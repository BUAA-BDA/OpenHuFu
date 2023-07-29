package com.hufudb.openhufu.benchmark;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.hufudb.openhufu.core.config.wyx_task.WXY_ConfigFile;
import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.owner.config.OwnerConfigFile;
import com.hufudb.openhufu.user.OpenHuFuUser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WXY_UserTest {

  public void printLine(ResultSet it) throws SQLException {
    for (int i = 1; i <= it.getMetaData().getColumnCount(); i++) {
      System.out.print(it.getString(i) + "|");
    }
    System.out.println();
  }

  @Test
  public void testSqlRangeQuery() throws SQLException, IOException {
    OpenHuFuUser user = new OpenHuFuUser();
    WXY_ConfigFile configFile = new Gson().fromJson(Files.newBufferedReader(
                    Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("tasks-rangeQuery.json")
                            .getPath())),
            WXY_ConfigFile.class);
    WXY_UserConfig userConfig = configFile.generateUserConfig();
    try (ResultSet dataset = user.executeTask(userConfig)) {
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      dataset.close();
      assertEquals(8, count);
    }
  }

  @Test
  public void testSqlRangeCount() throws SQLException, IOException {
    OpenHuFuUser user = new OpenHuFuUser();
    WXY_ConfigFile configFile = new Gson().fromJson(Files.newBufferedReader(
                    Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("tasks-rangeCount.json")
                            .getPath())),
            WXY_ConfigFile.class);
    WXY_UserConfig userConfig = configFile.generateUserConfig();
    try (ResultSet dataset = user.executeTask(userConfig)) {
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      dataset.close();
      assertEquals(1, count);
    }
  }

  @Test
  public void testSqlKNNQuery() throws SQLException, IOException {
    OpenHuFuUser user = new OpenHuFuUser();
    WXY_ConfigFile configFile = new Gson().fromJson(Files.newBufferedReader(
                    Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("tasks-KNN.json")
                            .getPath())),
            WXY_ConfigFile.class);
    WXY_UserConfig userConfig = configFile.generateUserConfig();
    try (ResultSet dataset = user.executeTask(userConfig)) {
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      dataset.close();
      assertEquals(10, count);
    }
  }
}
