package com.hufudb.openhufu.benchmark;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.hufudb.openhufu.benchmark.enums.SpatialTableName;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.user.OpenHuFuUser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenHuFuSpatialPostgisTest {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
  private static final OpenHuFuUser user = new OpenHuFuUser();

  @BeforeClass
  public static void setUp() throws IOException {
    LinkedTreeMap userConfigs = new Gson().fromJson(Files.newBufferedReader(
            Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("spatial-postgis-configs.json")
                .getPath())),
        LinkedTreeMap.class);
    List<String> endpoints = (List<String>) userConfigs.get("owners");
    List<GlobalTableConfig> globalTableConfigs =
        new Gson().fromJson(new Gson().toJson(userConfigs.get("tables")),
            new TypeToken<ArrayList<GlobalTableConfig>>() {
            }.getType());
    LOG.info("Init benchmark of OpenHuFuSpatialPOSTGIS...");
    for (String endpoint : endpoints) {
      user.addOwner(endpoint, null);
    }

    for (GlobalTableConfig config : globalTableConfigs) {
      user.createOpenHuFuTable(config);
    }
    LOG.info("Init finish");
  }

  public void printLine(ResultSet it) throws SQLException {
    for (int i = 1; i <= it.getMetaData().getColumnCount(); i++) {
      System.out.print(it.getString(i) + "|");
    }
    System.out.println();
  }

  @Test
  public void testSelect() throws SQLException {
    String sql = "select * from osm_a";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      int count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(400, count);
      dataset.close();
    }
  }

  @Test
  public void testSpatialDistance() throws SQLException {
    String sql = "select id, Distance(location, POINT(0, 0)) from osm_a";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(400, count);
      dataset.close();
    }
  }

  @Test
  public void testRangeQuery() throws SQLException {
    String sql = "select * from osm_a where DWithin(POINT(0, 0), location, 50)";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      dataset.close();
      assertEquals(30, count);
    }
  }

  /*
      Result: osm_a_1: 14, osm_a_2: 16, osm_a_3: 0, osm_a_4: 0
      Validation SQL:
      SELECT COUNT(*) from osm_a_1 where ST_DWithin('SRID=4326;POINT (0 0)', location, 50.0)
      SELECT COUNT(*) from osm_a_2 where ST_DWithin('SRID=4326;POINT (0 0)', location, 50.0)
      SELECT COUNT(*) from osm_a_3 where ST_DWithin('SRID=4326;POINT (0 0)', location, 50.0)
      SELECT COUNT(*) from osm_a_4 where ST_DWithin('SRID=4326;POINT (0 0)', location, 50.0)
  */
  @Test
  public void testRangeCount() throws SQLException {
    String sql = "select count(*) from osm_a where DWithin(POINT(0, 0), location, 50)";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      dataset.next();
      assertEquals(30, dataset.getInt(1));
      dataset.close();
    }
  }

  /*
    Valication SQL:
    SELECT id, location, distance
    FROM ((SELECT id                                   as id,
                  st_astext(location)                  as location,
                  'SRID=4326;POINT (0 0)' <-> location as distance
           FROM osm_a_1)
          union
          (SELECT id                                   as id,
                  st_astext(location)                  as location,
                  'SRID=4326;POINT (0 0)' <-> location as distance
           FROM osm_a_2)
          union
          (SELECT id                                   as id,
                  st_astext(location)                  as location,
                  'SRID=4326;POINT (0 0)' <-> location as distance
           FROM osm_a_3)
          union
          (SELECT id                                   as id,
                  st_astext(location)                  as location,
                  'SRID=4326;POINT (0 0)' <-> location as distance
           FROM osm_a_4)) AS new_osm_a
    ORDER BY distance
            ASC
    LIMIT 10
   */
  @Test
  public void testKNNQuery1() throws SQLException {
    String sql =
        "select id, location from osm_a order by Distance(POINT(0, 0), location) asc limit 10";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(10, count);
      dataset.close();
    }
  }

  @Test
  public void testKNNQuery2() throws SQLException {
    String sql = "select id, location from osm_a where KNN(POINT(0, 0), location, 10)";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(10, count);
      dataset.close();
    }
  }

  @Test
  public void testRangeJoin() throws SQLException {
    String sql =
        "select * from osm_b join osm_a on DWithin(osm_b.location, osm_a.location, 5)";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(220, count);
      dataset.close();
    }
  }
  
  @Test
  public void testKNNJOIN() throws SQLException {
    String sql = "select * from osm_b join osm_a on KNN(osm_b.location, osm_a.location, 5)";
    try (Statement stmt = user.createStatement()) {
      ResultSet dataset = stmt.executeQuery(sql);
      long count = 0;
      while (dataset.next()) {
        printLine(dataset);
        ++count;
      }
      assertEquals(200, count);
      dataset.close();
    }
  }
}
