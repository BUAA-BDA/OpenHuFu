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
 * use config files in resources/docker by default,
 * set $ONEDB_DISTRIBUTE environment variable to use config files in
 * resources/distribute
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TPCHOfficialBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(TPCHBenchmark.class);
    private static final OneDBState state = new OneDBState();

    public static class OneDBState {
        private OneDB cmd;
        boolean distribute = false;

        OneDBState() {
            try {
                setUp();
            } catch (IOException e) {
                throw new RuntimeException("Fail to setup");
            }
        }

        public void setUp() throws IOException {
            if (System.getenv("ONEDB_DISTRIBUTE") != null) {
                distribute = true;
            }
            try {
                Thread.sleep(3);
            } catch (Exception e) {
                LOG.error("Exception caught during setup", e);
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

        public void TearDown() {
            cmd.close();
            LOG.info("Close OneDB User Client");
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery1() {
        ResultSet resultSet =
                state.cmd.executeQuery("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '84' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery2() {
        ResultSet resultSet =
                state.cmd.executeQuery("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 16 and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery3() {
        ResultSet resultSet =
                state.cmd.executeQuery("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-06' and l_shipdate > date '1995-03-06' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery4() {
        ResultSet resultSet =
                state.cmd.executeQuery("select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery5() {
        ResultSet resultSet =
                state.cmd.executeQuery("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA' and o_orderdate >= date '1996-01-01' and o_orderdate < date '1996-01-01' + interval '1' year group by n_name order by revenue desc limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery6() {
        ResultSet resultSet =
                state.cmd.executeQuery("select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '1' year and l_discount between 0.05 - 0.01 and 0.05 + 0.01 and l_quantity < 25 limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery7() {
        ResultSet resultSet =
                state.cmd.executeQuery("select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'IRAN' and n2.n_name = 'ROMANIA') or (n1.n_name = 'ROMANIA' and n2.n_name = 'IRAN') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery8() {
        ResultSet resultSet =
                state.cmd.executeQuery("select o_year, sum(case when nation = 'ROMANIA' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'EUROPE' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'STANDARD ANODIZED COPPER' ) as all_nations group by o_year order by o_year limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery9() {
        ResultSet resultSet =
                state.cmd.executeQuery("select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%aquamarine%' ) as profit group by nation, o_year order by nation, o_year desc limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery10() {
        ResultSet resultSet =
                state.cmd.executeQuery("select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-03-01' and o_orderdate < date '1994-03-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery11() {
        ResultSet resultSet =
                state.cmd.executeQuery("select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' ) order by value desc limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery12() {
        ResultSet resultSet =
                state.cmd.executeQuery("select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('RAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1993-01-01' and l_receiptdate < date '1993-01-01' + interval '1' year group by l_shipmode order by l_shipmode limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery13() {
        ResultSet resultSet =
                state.cmd.executeQuery("select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) as c_count from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%pending%deposits%' group by c_custkey ) as c_orders group by c_count order by custdist desc, c_count desc limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery14() {
        ResultSet resultSet =
                state.cmd.executeQuery("select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1997-12-01' and l_shipdate < date '1997-12-01' + interval '1' month limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery15() {
        ResultSet resultSet =
                state.cmd.executeQuery("select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1997-12-01' and l_shipdate < date '1997-12-01' + interval '1' month limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery16() {
        ResultSet resultSet =
                state.cmd.executeQuery("select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#52' and p_type not like 'LARGE ANODIZED%' and p_size in (42, 38, 15, 48, 33, 3, 27, 45) and ps_suppkey not in ( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery17() {
        ResultSet resultSet =
                state.cmd.executeQuery("select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'WRAP CASE' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey ) limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery18() {
        ResultSet resultSet =
                state.cmd.executeQuery("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 314 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery19() {
        ResultSet resultSet =
                state.cmd.executeQuery("select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#54' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 5 and l_quantity <= 5 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#25' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 19 and l_quantity <= 19 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#42' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 24 and l_quantity <= 24 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery20() {
        ResultSet resultSet =
                state.cmd.executeQuery("select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'purple%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'UNITED KINGDOM' order by s_name limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery21() {
        ResultSet resultSet =
                state.cmd.executeQuery("select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'FRANCE' group by s_name order by numwait desc, s_name limit 100");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @Fork(0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void testQuery22() {
        ResultSet resultSet =
                state.cmd.executeQuery("select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode limit 1");
        int count = 0;
        try {
            while (resultSet.next()) {
                count++;
            }
            LOG.debug("equal join get {} rows", count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}



