package com.hufudb.openhufu.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.openhufu.benchmark.enums.SpatialTableName;
import com.hufudb.openhufu.benchmark.enums.TPCHTableName;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.user.OpenHuFuUser;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OpenHuFuSpatialBenchmarkTest {
  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
  private static final OpenHuFuUser user = new OpenHuFuUser();

  @BeforeClass
  public static void setUp() throws IOException {

    List<String> endpoints =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("spatial-endpoints.json")
                    .getPath())),
            new TypeToken<ArrayList<String>>() {
            }.getType());
    List<GlobalTableConfig> globalTableConfigs =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("spatial-tables.json")
                    .getPath())),
            new TypeToken<ArrayList<GlobalTableConfig>>() {
            }.getType());
    LOG.info("Init benchmark of OpenHuFuSpatial...");
    for (String endpoint : endpoints) {
      user.addOwner(endpoint, null);
    }

    for (GlobalTableConfig config : globalTableConfigs) {
      user.createOpenHuFuTable(config);
    }
    LOG.info("Init finish");
  }

  @Test
  public void testSelect() {
    String tableName = SpatialTableName.SPATIAL.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);
    plan.setSelectExps(ExpressionFactory
        .createInputRef(user.getOpenHuFuTableSchema(tableName).getSchema()));
    DataSet dataset = user.executeQuery(plan);
    DataSetIterator it = dataset.getIterator();
    long count = 0;
    while (it.next()) {
      for (int i = 0; i < it.size(); i++) {
        System.out.print(it.get(i) + "|");
      }
      System.out.println();
      ++count;
    }
    assertEquals(3000, count);
    dataset.close();
  }

  @Test
  public void testSpatialDistance() {
    String tableName = SpatialTableName.SPATIAL.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);
    plan.setSelectExps(ExpressionFactory.createInputRef(user.getOpenHuFuTableSchema(tableName).getSchema()));
    // select * from spatial where DWithin(S_POINT, Point(1404050.076199729, -4762163.267865509), 0.1);
    Expression pointFunc =
        ExpressionFactory.createScalarFunc(ColumnType.POINT, "Point",
            ImmutableList.of(ExpressionFactory.createLiteral(ColumnType.DOUBLE, 1404050.076199729),
                ExpressionFactory.createLiteral(ColumnType.DOUBLE, -4762163.267865509)));
    Expression dwithinFunc =
        ExpressionFactory.createScalarFunc(ColumnType.BOOLEAN, "DWithin",
            ImmutableList.of(ExpressionFactory.createInputRef(1, ColumnType.POINT, Modifier.PUBLIC),
                pointFunc, ExpressionFactory.createLiteral(ColumnType.DOUBLE, 0.1)));
    plan.setWhereExps(ImmutableList.of(dwithinFunc));
    DataSet dataset = user.executeQuery(plan);
    DataSetIterator it = dataset.getIterator();
    int count = 0;
    assertEquals(2, it.size());
    while (it.next()) {
      assertEquals(0L, it.get(0));
      count++;
    }
    assertEquals(1, count);
  }

}
