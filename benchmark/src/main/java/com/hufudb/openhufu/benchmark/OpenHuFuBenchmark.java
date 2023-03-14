package com.hufudb.openhufu.benchmark;

import com.google.common.collect.ImmutableList;
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
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.user.OpenHuFuUser;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.RootPlan;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    Path resourceDir = Paths.get(System.getenv("OPENHUFU_ROOT"), "benchmark", "src", "main", "resources");
    List<String> endpoints =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(String.valueOf(resourceDir), "endpoints.json")),
            new TypeToken<ArrayList<String>>() {
            }.getType());
    List<GlobalTableConfig> globalTableConfigs =
        new Gson().fromJson(Files.newBufferedReader(
                Path.of(String.valueOf(resourceDir), "tables.json")),
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
  public void testSelect() throws SQLException {
    String sql = "select * from nation";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testEqualJoin() throws SQLException {
    String sql = "select * from nation join supplier on nation.N_NATIONKEY = supplier.S_NATIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testLeftJoin() throws SQLException {
    String sql = "select * from nation left join supplier on nation.N_NATIONKEY = supplier.S_NATIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testRightJoin() throws SQLException {
    String sql = "select * from nation right join supplier on nation.N_NATIONKEY = supplier.S_NATIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testOuterJoin() throws SQLException {
    String sql = "select * from nation outer join supplier on nation.N_NATIONKEY = supplier.S_NATIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testCount() throws SQLException {
    String sql = "select count(*) from supplier";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testAvg() throws SQLException {
    String sql = "select avg(S_SUPPKEY) from supplier";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testSum() throws SQLException {
    String sql = "select sum(S_SUPPKEY) from supplier";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testMax() throws SQLException {
    String sql = "select max(S_SUPPKEY) from supplier";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testMin() throws SQLException {
    String sql = "select min(S_SUPPKEY) from supplier";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testGroupByAndOrder() throws SQLException {
    String sql = "select count(S_SUPPKEY) from supplier group by S_NATIONKEY order by S_NATIONKEY DESC";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
}



