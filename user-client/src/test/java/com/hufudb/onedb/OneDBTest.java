package com.hufudb.onedb;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.table.TableMeta;
import junit.framework.TestCase;
import org.junit.Test;

import java.net.URL;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneDBTest extends TestCase {

  static OneDB oneDB;

  public void setUp() throws Exception {
    super.setUp();
    oneDB = new OneDB();
    ClassLoader classLoader = OneDBTest.class.getClassLoader();
    URL resource = classLoader.getResource("ci/ca.pem");
    List<String> endpoints = ImmutableList.of("localhost:12345", "localhost:12346", "localhost:12347");
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
    List<FieldType> header = Arrays.asList(FieldType.STRING, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql1.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

}