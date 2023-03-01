package com.hufudb.openhufu.benchmark;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.openhufu.benchmark.enums.TPCHTableName;
import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.core.client.OpenHuFuClient;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuTable;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaFactory;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.user.OpenHuFuUser;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.RootPlan;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.schema.SchemaPlus;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query benchmark: Require that all data owners have been started,
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class OpenHuFuBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
  private static final OpenHuFuUser user = new OpenHuFuUser();

  @Setup
  public void setUp() throws IOException {

    List<String> endpoints =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("endpoints.json")
                    .getPath())),
            new TypeToken<ArrayList<String>>() {
            }.getType());
    List<GlobalTableConfig> globalTableConfigs =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("tables.json")
                    .getPath())),
            new TypeToken<ArrayList<GlobalTableConfig>>() {
            }.getType());
    LOG.info("Init benchmark of OpenHuFu...");
    for (String endpoint : endpoints) {
      user.addOwner(endpoint, null);
    }

    for (GlobalTableConfig config : globalTableConfigs) {
      user.createOpenHuFuTable(config);
    }
    LOG.info("Init finish");
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testSelect() {
    String tableName = TPCHTableName.NATION.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);
    plan.setSelectExps(ExpressionFactory
        .createInputRef(user.getOpenHuFuTableSchema(tableName).getSchema()));
    DataSet dataSet = user.executeQuery(plan);
    dataSet.close();
  }

}



