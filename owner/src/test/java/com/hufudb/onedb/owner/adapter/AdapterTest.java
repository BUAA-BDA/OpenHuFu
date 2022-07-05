package com.hufudb.onedb.owner.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.onedb.data.storage.utils.DateUtils;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

@RunWith(JUnit4.class)
public class AdapterTest {

  static List<PojoPublishedTableSchema> publishedSchemas;
  static Adapter adapter;
  static SchemaManager manager;

  @BeforeClass
  public static void setUp() {
    boolean useDocker = true;
    if (System.getenv("ONEDB_TEST_LOCAL") != null) {
      useDocker = false;
    }
    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("student");
    t1.setPublishedName("student1");
    t1.setPublishedColumns(ImmutableList.of());
    t1.setActualColumns(ImmutableList.of());
    PojoPublishedTableSchema t2 = new PojoPublishedTableSchema();
    t2.setActualName("student");
    t2.setPublishedName("student2");
    t2.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("DeptName", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Score", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Name", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Age", ColumnTypeWrapper.INT, ModifierWrapper.HIDDEN)));
    t2.setActualColumns(ImmutableList.of(3, 2, 0, 1));
    publishedSchemas = ImmutableList.of(t1, t2);
    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.datasource = "postgresql";
    if (useDocker) {
      adapterConfig.url = "jdbc:postgresql://postgres1:5432/postgres";
    } else {
      adapterConfig.url = "jdbc:postgresql://localhost:13101/postgres";
    }
    adapterConfig.catalog = "postgres";
    adapterConfig.user = "postgres";
    adapterConfig.passwd = "onedb";
    String onedbRoot = System.getenv("ONEDB_ROOT");
    assertNotNull("ONEDB_ROOT env variable is not set", onedbRoot);
    String adapterDir = Paths.get(onedbRoot, "adapter").toString();
    adapter = AdapterFactory.loadAdapter(adapterConfig, adapterDir);
    manager = adapter.getSchemaManager();
    for (PojoPublishedTableSchema schema : publishedSchemas) {
      assertTrue(manager.addPublishedTable(schema));
    }
    PojoPublishedTableSchema t3 = new PojoPublishedTableSchema();
    t3.setActualName("taxi");
    t3.setPublishedName("taxi");
    t3.setPublishedColumns(ImmutableList.of());
    t3.setActualColumns(ImmutableList.of());
    assertTrue(manager.addPublishedTable(t3));
  }


  @Test
  public void testAdapterBasic() {
    // test query select * from student1;
    LeafPlan plan = new LeafPlan();
    plan.setTableName("student1");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("student1")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    int count = 0;
    while (it.next()) {
      assertEquals(4, it.size());
      count++;
    }
    assertTrue(count > 0);
    result.close();
    // test query select * from student1 where score >= 90;
    plan.setWhereExps(ImmutableList.of(ExpressionFactory.createBinaryOperator(OperatorType.GE,
        ColumnType.BOOLEAN, ExpressionFactory.createInputRef(2, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.INT, 90))));
    result = adapter.query(plan);
    it = result.getIterator();
    while (it.next()) {
      assertTrue((int) it.get(2) >= 90);
    }
    // test query select dept_name, score from student1 where score >= 90;
    plan.setSelectExps(
        ImmutableList.of(ExpressionFactory.createInputRef(3, ColumnType.STRING, Modifier.PUBLIC),
            ExpressionFactory.createInputRef(2, ColumnType.INT, Modifier.PUBLIC)));
    result = adapter.query(plan);
    it = result.getIterator();
    while (it.next()) {
      assertTrue((int) it.get(1) >= 90);
    }
    result.close();
    // test query select dept_name, AVG(score) from student1 where score >= 90;
    plan.setAggExps(ImmutableList.of(
        ExpressionFactory.createAggFunc(ColumnType.STRING, Modifier.PUBLIC, 0,
            ImmutableList
                .of(ExpressionFactory.createInputRef(0, ColumnType.STRING, Modifier.PUBLIC))),
        ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC, 2, ImmutableList
            .of(ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC)))));
    plan.setGroups(ImmutableList.of(0));
    result = adapter.query(plan);
    it = result.getIterator();
    while (it.next()) {
      assertTrue((int) it.get(1) >= 90);
    }
    result.close();
    // todo: test more query plan
  }

  @Test
  public void testAdapterWithSchemaMapping() {
    // student2: [dept_name, score, name]
    // test query select * from student2;
    LeafPlan plan = new LeafPlan();
    plan.setTableName("student2");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("student2")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    int count = 0;
    while (it.next()) {
      assertEquals(3, it.size());
      count++;
    }
    assertTrue(count > 0);
    // test query select * from student2 where score >= 90;
    plan.setWhereExps(ImmutableList.of(ExpressionFactory.createBinaryOperator(OperatorType.GE,
        ColumnType.BOOLEAN, ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.INT, 90))));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      count++;
      assertTrue((int) it.get(1) >= 90);
    }
    assertTrue(count > 0);
    result.close();
    // test query select dept_name, score from student1 where score >= 90;
    plan.setSelectExps(
        ImmutableList.of(ExpressionFactory.createInputRef(0, ColumnType.STRING, Modifier.PUBLIC),
            ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC)));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      count++;
      assertTrue((int) it.get(1) >= 90);
    }
    assertTrue(count > 0);
    result.close();
    // test query select dept_name, AVG(score) from student1 where score >= 90 group by dept_name;
    plan.setAggExps(ImmutableList.of(
        ExpressionFactory.createAggFunc(ColumnType.STRING, Modifier.PUBLIC, 0,
            ImmutableList
                .of(ExpressionFactory.createInputRef(0, ColumnType.STRING, Modifier.PUBLIC))),
        ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC, 2, ImmutableList
            .of(ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC)))));
    plan.setGroups(ImmutableList.of(0));
    result = adapter.query(plan);
    it = result.getIterator();
    while (it.next()) {
      assertTrue((int) it.get(1) >= 90);
    }
    result.close();
  }
  
  @Test
  public void testDateTypes() {
    LeafPlan plan = new LeafPlan();
    plan.setTableName("taxi");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("taxi")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    assertTrue(it.next());
    assertEquals(Date.valueOf("2018-09-01").toString(), it.get(1).toString());
    assertEquals(Time.valueOf("09:05:10").toString(), it.get(2).toString());
    assertEquals(Timestamp.valueOf("2018-09-01 09:05:10").toString(), it.get(3).toString());
    assertTrue(it.next());
    assertEquals(Date.valueOf("2018-06-01").toString(), it.get(1).toString());
    assertEquals(Time.valueOf("10:14:45").toString(), it.get(2).toString());
    assertEquals(Timestamp.valueOf("2018-06-01 10:14:45").toString(), it.get(3).toString());
    assertTrue(it.next());
    assertEquals(Date.valueOf("2019-01-30").toString(), it.get(1).toString());
    assertEquals(Time.valueOf("21:31:20").toString(), it.get(2).toString());
    assertEquals(Timestamp.valueOf("2019-01-30 21:31:20").toString(), it.get(3).toString());
    assertFalse(it.next());
  }
}
