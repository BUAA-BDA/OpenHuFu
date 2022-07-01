/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved. DO NOT ALTER OR
 * REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it under the terms of the GNU
 * General Public License version 2 only, as published by the Free Software Foundation. Oracle
 * designates this particular file as subject to the "Classpath" exception as provided by Oracle in
 * the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version 2 along with this work;
 * if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA or visit www.oracle.com
 * if you need additional information or have any questions.
 */

package com.hufudb.onedb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.user.OneDB;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query benchmark:
 * Require that all data owners have been started,
 * use config files in resources/docker by default,
 * set $ONEDB_DISTRIBUTE environment variable to use config files in
 * resources/distribute
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TPCHBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(TPCHBenchmark.class);

  @State(Scope.Benchmark)
  public static class OneDBState {
    private OneDB cmd;
    boolean distribute = false;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
      if (System.getenv("ONEDB_DISTRIBUTE") != null) {
        distribute = true;
      }
      try {
        Thread.sleep(3);
      } catch (Exception e) {
        e.printStackTrace();
      }
      List<String> endpoints =
          new Gson().fromJson(Files.newBufferedReader(Paths.get("docker/endpoints.json")),
              new TypeToken<ArrayList<String>>() {}.getType());
      List<GlobalTableConfig> globalTableConfigs =
          new Gson().fromJson(Files.newBufferedReader(Paths.get("docker/tables.json")),
              new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
      LOG.info("Init benchmark of OneDB...");
      cmd = new OneDB();
      for (String endpoint : endpoints) {
        cmd.addOwner(endpoint);
      }
      for (GlobalTableConfig config : globalTableConfigs) {
        cmd.createOneDBTable(config);
      }
      LOG.info("Init finish");
    }

    @TearDown(Level.Trial)
    public void TearDown() {
      cmd.close();
      LOG.info("Close OneDB User Client");
    }
  }


  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  public void testAggregate(OneDBState state) {
    ResultSet resultSet = state.cmd.executeQuery("select l_shipmode, "
        + "l_extendedprice * (1 - l_discount) * (1 + l_tax), "
        + "l_extendedprice * (1 - l_discount) * (1 + l_tax), " + "l_quantity " + "from lineitem "
        + "where  l_shipdate <= 755366400 and l_extendedprice * l_tax >= 230");
    try {
      while (resultSet.next()) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  public void testGroupBy(OneDBState state) {
    ResultSet resultSet =
        state.cmd.executeQuery("select  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty, "
            + " sum(l_extendedprice) as sum_base_price, "
            + " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,  "
            + "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  "
            + "avg(l_quantity) as avg_qty,  avg(l_extendedprice) as avg_price,  "
            + "avg(l_discount) as avg_disc,  count(*) as count_order " + "from lineitem "
            + "group by  l_returnflag,  l_linestatus");
    try {
      while (resultSet.next()) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  public void testSortLimit(OneDBState state) {
    ResultSet resultSet =
        state.cmd.executeQuery("select l_orderkey, l_extendedprice " +
        "from  lineitem " +
        "order by  l_shipmode ASC,  " +
        "l_shipinstruct DESC,  " +
        "l_extendedprice * l_discount / l_quantity ASC , l_orderkey " +
        "limit 100 offset 20");
    try {
      while (resultSet.next()) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  @Fork(0)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  public void testJoin(OneDBState state) {
    ResultSet resultSet =
        state.cmd.executeQuery("select  c_name,  c_custkey,  o_orderkey,  " +
        "o_orderdate,  o_totalprice " +
        "from  customer,  orders,  lineitem " +
        "where  c_custkey = o_custkey  and o_orderkey = l_orderkey");
    try {
      while (resultSet.next()) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
