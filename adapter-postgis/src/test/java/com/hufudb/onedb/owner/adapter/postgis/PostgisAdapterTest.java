package com.hufudb.onedb.owner.adapter.postgis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.nio.file.Paths;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.expression.ScalarFuncType;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class PostgisAdapterTest {
  static List<PojoPublishedTableSchema> publishedSchemas;
  static PostgisAdapter adapter;
  static SchemaManager manager;

  @BeforeClass
  public static void setUp() {
    boolean useDocker = true;
    if (System.getenv("ONEDB_TEST_LOCAL") != null) {
      useDocker = false;
    }
    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.datasource = "postgis";
    if (useDocker) {
      adapterConfig.url = "jdbc:postgresql://htpostgis5:5432/postgres";
    } else {
      adapterConfig.url = "jdbc:postgresql://localhost:13112/postgres";
    }
    adapterConfig.catalog = "postgres";
    adapterConfig.user = "postgres";
    adapterConfig.passwd = "onedb";
    String onedbRoot = System.getenv("ONEDB_ROOT");
    assertNotNull("ONEDB_ROOT env variable is not set", onedbRoot);
    String adapterDir = Paths.get(onedbRoot, "adapter").toString();
    adapter = (PostgisAdapter) AdapterFactory.loadAdapter(adapterConfig, adapterDir);
    manager = adapter.getSchemaManager();

    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("traffic");
    t1.setPublishedName("traffic");
    t1.setPublishedColumns(ImmutableList.of());
    t1.setActualColumns(ImmutableList.of());
    publishedSchemas = ImmutableList.of(t1);
    for (PojoPublishedTableSchema schema : publishedSchemas) {
      assertTrue(manager.addPublishedTable(schema));
    }
  }

  @Test
  public void testGenerateSQL() {
    LeafPlan plan = new LeafPlan();
    // select * from traffic;
    plan.setTableName("traffic");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("traffic")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    int count = 0;
    while (it.next()) {
      assertEquals(2, it.size());
      count++;
    }
    assertEquals(9, count);
    // select * from traffic where DWithin(location, Point(116.0, 40.0, 0.5), 0.5);
    Expression pointFunc = ExpressionFactory.createScalarFunc(ColumnType.POINT, ScalarFuncType.POINT.getId(), ImmutableList.of(
      ExpressionFactory.createLiteral(ColumnType.DOUBLE, 116.0),
      ExpressionFactory.createLiteral(ColumnType.DOUBLE, 40.0)
    ));
    Expression dwithinFunc = ExpressionFactory.createScalarFunc(ColumnType.BOOLEAN, ScalarFuncType.DWITHIN.getId(), ImmutableList.of(
      ExpressionFactory.createInputRef(1, ColumnType.POINT, Modifier.PUBLIC),
      pointFunc,
      ExpressionFactory.createLiteral(ColumnType.DOUBLE, 0.5)
    ));
    plan.setWhereExps(ImmutableList.of(dwithinFunc));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      assertEquals(2, it.size());
      count++;
    }
    assertEquals(3, count);
    // select * from traffic where Distance(location, Point(116.0, 40.0, 0.5)) <= 0.5;
    Expression distanceFunc =  ExpressionFactory.createScalarFunc(ColumnType.DOUBLE, ScalarFuncType.DISTANCE.getId(), ImmutableList.of(
      ExpressionFactory.createInputRef(1, ColumnType.POINT, Modifier.PUBLIC),
      pointFunc
    ));
    Expression cmp = ExpressionFactory.createBinaryOperator(OperatorType.LE, ColumnType.BOOLEAN, distanceFunc, ExpressionFactory.createLiteral(ColumnType.DOUBLE, 0.5));
    plan.setWhereExps(ImmutableList.of(cmp));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      assertEquals(2, it.size());
      count++;
    }
    assertEquals(3, count);
  }
}
