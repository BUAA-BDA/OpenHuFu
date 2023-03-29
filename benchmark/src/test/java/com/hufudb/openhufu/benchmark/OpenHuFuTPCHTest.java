package com.hufudb.openhufu.benchmark;

import static org.junit.Assert.assertEquals;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.user.OpenHuFuUser;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;

public class OpenHuFuTPCHTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
    private static final OpenHuFuUser user = new OpenHuFuUser();

    @BeforeClass
    public static void setUp() throws IOException {

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

    public void printLine(ResultSet it) throws SQLException {
        for (int i = 1; i <= it.getMetaData().getColumnCount(); i++) {
            System.out.print(it.getString(i) + "|");
        }
        System.out.println();
    }

    @Test
    public void testQuery1() throws SQLException {
        String sql = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '84' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery2() throws SQLException {
        //todo hash-equal-join only support 2 parties
        String sql = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 16 and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery3() throws SQLException {
        String sql = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-06' and l_shipdate > date '1995-03-06' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(10, count);
        it.close();
    }

    @Test
    public void testQuery4() throws SQLException {
        String sql = "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery5() throws SQLException {
        //todo hash-equal-join only support 2 parties
        String sql = "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA' and o_orderdate >= date '1996-01-01' and o_orderdate < date '1996-01-01' + interval '1' year group by n_name order by revenue desc limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery6() throws SQLException {
        //todo not support EXTRACT(FLAG(YEAR), $6)
        String sql = "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '1' year and l_discount between 0.05 - 0.01 and 0.05 + 0.01 and l_quantity < 25 limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery7() throws SQLException {
        //todo not support EXTRACT(FLAG(YEAR), $6)
        String sql = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'IRAN' and n2.n_name = 'ROMANIA') or (n1.n_name = 'ROMANIA' and n2.n_name = 'IRAN') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery8() throws SQLException {
        //todo not support EXTRACT(FLAG(YEAR), $11)
        String sql = "select o_year, sum(case when nation = 'ROMANIA' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'EUROPE' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'STANDARD ANODIZED COPPER' ) as all_nations group by o_year order by o_year limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery9() throws SQLException {
        //todo not support EXTRACT(FLAG(YEAR), $14)
        String sql = "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%aquamarine%' ) as profit group by nation, o_year order by nation, o_year desc limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery10() throws SQLException {
        String sql = "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-03-01' and o_orderdate < date '1994-03-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(20, count);
        it.close();
    }

    @Test
    public void testQuery11() throws SQLException {
        //todo SINGLE_VALUE is unsupported by openhufu
        String sql = "select ps_partkey, sum(ps_supplycost * ps_availqty) as v from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' ) order by v desc limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery12() throws SQLException {
        //todo not support IS TRUE(SEARCH($1, Sarg['1-URGENT':VARCHAR, '2-HIGH':VARCHAR]:VARCHAR))
        String sql = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('RAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1993-01-01' and l_receiptdate < date '1993-01-01' + interval '1' year group by l_shipmode order by l_shipmode limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(3179, count);
        it.close();
    }

    @Test
    public void testQuery13() throws SQLException {
        String sql = "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) as c_count from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%pending%deposits%' group by c_custkey ) as c_orders group by c_count order by custdist desc, c_count desc limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery14() throws SQLException {
        String sql = "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1997-12-01' and l_shipdate < date '1997-12-01' + interval '1' month limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery15() throws SQLException {
        //todo not support
        String sql = "";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery16() throws SQLException {
        //todo Missing conversion is LogicalFilter
        String sql = "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#52' and p_type not like 'LARGE ANODIZED%' and p_size in (42, 38, 15, 48, 33, 3, 27, 45) and ps_suppkey not in ( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery17() throws SQLException {
        //todo hash-equal-join only support 2 parties
        String sql = "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'WRAP CASE' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey ) limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery18() throws SQLException {
        //todo Missing conversions are LogicalFilter[convention: NONE -> OpenHuFu] (2 cases), LogicalAggregate[convention: NONE -> OpenHuFu, sort: [] -> [3 DESC, 4]]
        String sql = "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 314 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery19() throws SQLException {
        //todo HashEqualJoin not support theta join
        String sql = "select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#54' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 5 and l_quantity <= 5 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#25' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 19 and l_quantity <= 19 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#42' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 24 and l_quantity <= 24 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery20() throws SQLException {
        //todo hash-equal-join only support 2 parties
        String sql = "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'purple%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'UNITED KINGDOM' order by s_name limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery21() throws SQLException {
        //todo Missing conversion is LogicalFilter[convention: NONE -> OpenHuFu]
        String sql = "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'FRANCE' group by s_name order by numwait desc, s_name limit 100";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

    @Test
    public void testQuery22() throws SQLException {
        //todo Missing conversion is LogicalFilter[convention: NONE -> OpenHuFu]
        String sql = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode limit 1";
        ResultSet it = user.executeQuery(sql);
        int count = 0;
        while (it.next()) {
            printLine(it);
            ++count;
        }
        assertEquals(1, count);
        it.close();
    }

}
