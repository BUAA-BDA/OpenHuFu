package com.hufudb.openhufu.benchmark;

import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.RootPlan;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query benchmark:
 * Require that all data owners have been started,
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class OpenHuFuBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
  private static final OpenHuFuState state = new OpenHuFuState();

  public static class OpenHuFuState {
    private OpenHuFuSchemaManager manager;

    OpenHuFuState() {
      try {
        setUp();
      } catch (IOException e) {
        throw new RuntimeException("Fail to setup");
      }
    }

    public void setUp() throws IOException {
      try {
        Thread.sleep(3);
      } catch (Exception e) { //NOSONAR
        LOG.error("Exception caught during setup", e);
      }

      LOG.info("Init finish");
    }
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  public void testAggregate() {
    Plan plan = new RootPlan();
    DataSet dataSet = state.manager.query(plan);
    dataSet.close();
  }



}



