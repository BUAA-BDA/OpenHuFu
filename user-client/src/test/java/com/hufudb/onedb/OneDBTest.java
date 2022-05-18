package com.hufudb.onedb;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.table.TableMeta;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneDBTest {

  static OneDB oneDB;

  @Before
  public void setUp() {
    oneDB = new OneDB();
    ClassLoader classLoader = OneDBTest.class.getClassLoader();
    URL resource = classLoader.getResource("ci_crt/ca.pem");
    List<String> endpoints = ImmutableList.of("owner1:12345", "owner2:12345", "owner3:12345");
    endpoints.forEach(e -> {
      assert resource != null;
      oneDB.addOwner(e, resource.getPath());
    });
    oneDB.createOneDBTable(new TableMeta("region",
            Stream.of(endpoints.get(0)).map(e -> new TableMeta.LocalTableMeta(e, "region"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("nation",
            Stream.of(endpoints.get(0)).map(e -> new TableMeta.LocalTableMeta(e, "nation"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("partsupp",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "partsupp"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("customer",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "customer"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("lineitem",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "lineitem"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("orders",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "orders"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("part",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "part"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new TableMeta("supplier",
            endpoints.stream().map(e -> new TableMeta.LocalTableMeta(e, "supplier"))
                    .collect(Collectors.toList())));
  }

  @Test
  public void testProject() {
    ResultSet rs = oneDB.executeQuery("select l_shipmode, " +
            "l_extendedprice * (1 - l_discount) * (1 + l_tax), " +
            "l_extendedprice * (1 - l_discount) * (1 + l_tax), " +
            "l_quantity " +
            "from lineitem " +
            "where  l_shipdate <= 755366400 and l_extendedprice * l_tax >= 230");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql1.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testCaseWhen() {
    ResultSet rs = oneDB.executeQuery("select case  when o_orderpriority = '1-URGENT' " +
            "or o_orderpriority = '2-HIGH'  then 1  else 0  end " +
            "from  orders  where  " +
            "o_totalprice * o_totalprice / o_totalprice < 90000");
    List<ColumnType> header = Collections.singletonList(ColumnType.INT);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql2.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testDistinct() {
    ResultSet rs = oneDB.executeQuery("select distinct l_orderkey " +
            "from  lineitem where   " +
            "l_commitdate < l_receiptdate or l_discount > l_tax");
    List<ColumnType> header = Collections.singletonList(ColumnType.INT);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql3.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testAggregate() {
    ResultSet rs = oneDB.executeQuery("select  sum(l_quantity),  sum(l_extendedprice),  " +
            "sum(l_extendedprice * (1 - l_discount)),  " +
            "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),  " +
            "avg(l_extendedprice / (l_extendedprice * (1 - l_discount) * (1 + l_tax))),  " +
            "max(l_extendedprice) * min(l_extendedprice * (1 - l_discount)),  " +
            "count(*) from  lineitem");
    List<ColumnType> header = Arrays.asList(ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql4.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testGroupBy() {
    ResultSet rs = oneDB.executeQuery("select  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty, " +
            " sum(l_extendedprice) as sum_base_price, " +
            " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,  " +
            "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  " +
            "avg(l_quantity) as avg_qty,  avg(l_extendedprice) as avg_price,  " +
            "avg(l_discount) as avg_disc,  count(*) as count_order " +
            "from  lineitem " +
            "group by  l_returnflag,  l_linestatus");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.STRING ,ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql5.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testSortLimit() {
    ResultSet rs = oneDB.executeQuery("select l_orderkey, l_extendedprice " +
            "from  lineitem " +
            "order by  l_shipmode ASC,  " +
            "l_shipinstruct DESC,  " +
            "l_extendedprice * l_discount / l_quantity ASC , l_orderkey " +
            "limit 100 offset 20");
    List<ColumnType> header = Arrays.asList(ColumnType.INT, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql6.csv", header);
    assert resultDataSet.compareWithOrder();
  }

  @Test
  public void testJoin() {
    ResultSet rs = oneDB.executeQuery("select  c_name,  c_custkey,  o_orderkey,  " +
            "o_orderdate,  o_totalprice " +
            "from  customer,  orders,  lineitem " +
            "where  c_custkey = o_custkey  and o_orderkey = l_orderkey");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.INT, ColumnType.INT,
            ColumnType.INT, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql7.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }
}