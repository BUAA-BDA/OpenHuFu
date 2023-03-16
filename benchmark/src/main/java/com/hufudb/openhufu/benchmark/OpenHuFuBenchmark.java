package com.hufudb.openhufu.benchmark;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.user.OpenHuFuUser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    ResultSet it = user.executeQuery(sql);
    it.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testEqualJoin() throws SQLException {
    String sql = "select * from nation join region on nation.N_REGIONKEY = region.R_REGIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testLeftJoin() throws SQLException {
    String sql = "select * from nation left join region on nation.N_REGIONKEY = region.R_REGIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testRightJoin() throws SQLException {
    String sql = "select * from nation right join region on nation.N_REGIONKEY = region.R_REGIONKEY";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
public void testFullJoin() throws SQLException {
  String sql = "select * from nation full join region on nation.N_REGIONKEY = region.R_REGIONKEY";
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
    String sql = "select avg(P_PARTKEY) from part";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testSum() throws SQLException {
    String sql = "select sum(P_PARTKEY) from part";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testMax() throws SQLException {
    String sql = "select max(C_CUSTKEY) from customer";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testMin() throws SQLException {
    String sql = "select min(C_CUSTKEY) from customer";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 1)
  public void testGroupByAndOrder() throws SQLException {
    String sql = "select count(C_CUSTKEY) from customer group by C_CUSTKEY order by C_CUSTKEY DESC";
    ResultSet dataSet = user.executeQuery(sql);
    dataSet.close();
  }
}



