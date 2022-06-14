package com.hufudb.onedb.owner.adapter.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.net.URL;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class CsvAdapterTest {

  @Test
  public void testQuery() {
    URL source = CsvAdapterTest.class.getClassLoader().getResource("data");
    CsvAdapterFactory factory = new CsvAdapterFactory();
    AdapterConfig config = new AdapterConfig();
    config.url = source.getPath();
    config.datasource = "csv";
    Adapter adapter = factory.create(config);
    // add published schema
    SchemaManager manager = adapter.getSchemaManager();
    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("test2");
    t1.setPublishedName("student1");
    t1.setPublishedColumns(ImmutableList.of());
    t1.setActualColumns(ImmutableList.of());
    PojoPublishedTableSchema t2 = new PojoPublishedTableSchema();
    t2.setActualName("test2");
    t2.setPublishedName("student2");
    t2.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("DeptName", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Score", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Name", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("Weight", ColumnTypeWrapper.DOUBLE, ModifierWrapper.PUBLIC)));
    t2.setActualColumns(ImmutableList.of(3, 2, 0, 4));
    manager.addPublishedTable(t1);
    manager.addPublishedTable(t2);
    LeafPlan plan = new LeafPlan();
    plan.setTableName("student2");
    // select * from student2;
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("student2")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    int count = 0;
    while (it.next()) {
      assertEquals(4, it.size());
      count++;
    }
    assertEquals(3, count);
    result.close();
    // select Name, Weight * 2, Score from student2;
    plan.setSelectExps(
        ImmutableList.of(ExpressionFactory.createInputRef(2, ColumnType.STRING, Modifier.PUBLIC),
            ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE,
                ExpressionFactory.createInputRef(3, ColumnType.DOUBLE, Modifier.PUBLIC),
                ExpressionFactory.createLiteral(ColumnType.INT, 2)),
            ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC)));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      assertEquals(3, it.size());
      count++;
    }
    assertEquals(3, count);
    result.close();
    // select Name, Weight * 2, Score from student2 where Score >= 90;
    plan.setWhereExps(ImmutableList.of(ExpressionFactory.createBinaryOperator(OperatorType.GE,
        ColumnType.BOOLEAN, ExpressionFactory.createInputRef(2, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.INT, 90))));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      count++;
    }
    assertEquals(2, count);
    result.close();
    // select Name, AVG(Weight * 2), Score from student2 where Score>= 90 group by Name
    plan.setAggExps(ImmutableList.of(
        ExpressionFactory.createAggFunc(ColumnType.STRING, Modifier.PUBLIC, 0,
            ImmutableList
                .of(ExpressionFactory.createInputRef(0, ColumnType.STRING, Modifier.PUBLIC))),
        ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC, 2, ImmutableList
            .of(ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC)))));
    plan.setGroups(ImmutableList.of(0));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals("tom", it.get(0));
    assertEquals(151.0, ((Number) it.get(1)).doubleValue(), 0.001);
    assertTrue(it.next());
    assertEquals("Snow", it.get(0));
    assertEquals(138.2, ((Number) it.get(1)).doubleValue(), 0.001);
    assertFalse(it.next());
    result.close();
    adapter.shutdown();
  }
}
