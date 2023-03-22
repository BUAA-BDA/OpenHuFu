package com.hufudb.openhufu.owner.adapter.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.hufudb.openhufu.common.enums.DataSourceType;
import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import java.net.URL;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.schema.utils.PojoColumnDesc;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.adapter.AdapterConfig;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;

public class CsvAdapterTest {

  @Test
  public void testLoadSingleFile() {
    URL source = CsvAdapterTest.class.getClassLoader().getResource("data");
    CsvAdapterFactory factory = new CsvAdapterFactory();
    AdapterConfig config = new AdapterConfig();
    config.url = source.getPath();
    config.datasource = DataSourceType.CSV;
    config.delimiter = ",";
    Adapter adapter = factory.create(config);
    SchemaManager manager = adapter.getSchemaManager();
    List<TableSchema> schemas = manager.getAllLocalTable();
    assertEquals(4, schemas.size());
    assertEquals(new HashSet<>() {{
                   add("test1");
                   add("test2");
                   add("test3");
                   add("test4");
                 }},
        schemas.stream().map(TableSchema::getName).collect(Collectors.toSet()));
  }

  @Test
  public void testQuery() {
    URL source = CsvAdapterTest.class.getClassLoader().getResource("data");
    CsvAdapterFactory factory = new CsvAdapterFactory();
    AdapterConfig config = new AdapterConfig();
    config.url = source.getPath();
    config.datasource = DataSourceType.CSV;
    config.delimiter = ",";
    Adapter adapter = factory.create(config);
    // add published schema
    SchemaManager manager = adapter.getSchemaManager();
    assertEquals(4, manager.getAllLocalTable().size());
    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("test2");
    t1.setPublishedName("student1");
    t1.setPublishedColumns(ImmutableList.of());
    PojoPublishedTableSchema t2 = new PojoPublishedTableSchema();
    t2.setActualName("test2");
    t2.setPublishedName("student2");
    t2.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("DeptName", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC, 3),
        new PojoColumnDesc("Score", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC, 2),
        new PojoColumnDesc("Name", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC, 0),
        new PojoColumnDesc("Weight", ColumnTypeWrapper.DOUBLE, ModifierWrapper.PUBLIC, 4)));
    PojoPublishedTableSchema t3 = new PojoPublishedTableSchema();
    t3.setActualName("test3");
    t3.setPublishedName("datetime");
    t3.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("license", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC, 0),
        new PojoColumnDesc("cur_date", ColumnTypeWrapper.DATE, ModifierWrapper.PUBLIC, 1),
        new PojoColumnDesc("cur_time", ColumnTypeWrapper.TIME, ModifierWrapper.PUBLIC, 2),
        new PojoColumnDesc("time_stamp", ColumnTypeWrapper.TIMESTAMP, ModifierWrapper.PUBLIC, 3)
    ));
    PojoPublishedTableSchema t4 = new PojoPublishedTableSchema();
    t4.setActualName("test4");
    t4.setPublishedName("traffic");
    t4.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("id", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC, 0),
        new PojoColumnDesc("location", ColumnTypeWrapper.GEOMETRY, ModifierWrapper.PUBLIC, 1)));
    manager.addPublishedTable(t1);
    manager.addPublishedTable(t2);
    manager.addPublishedTable(t3);
    manager.addPublishedTable(t4);
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
    assertEquals(4, count);
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
    assertEquals(4, count);
    result.close();
    // select Name, Weight * 2, Score from student2 where DeptName = 'computer';
    plan.setWhereExps(ImmutableList.of(ExpressionFactory.createBinaryOperator(OperatorType.EQ,
        ColumnType.BOOLEAN, ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.STRING, "computer"))));
    result = adapter.query(plan);
    it = result.getIterator();
    count = 0;
    while (it.next()) {
      count++;
    }
    assertEquals(1, count);
    result.close();
    // select Name, Weight * 2, Score from student2 where Score >= 90;
    plan.setWhereExps(ImmutableList.of(ExpressionFactory.createBinaryOperator(OperatorType.GE,
        ColumnType.BOOLEAN, ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PUBLIC),
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
    // select * from datetime;
    plan = new LeafPlan();
    plan.setTableName("datetime");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("datetime")));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals("10000", it.get(0));
    assertEquals(Date.valueOf("2018-09-01"), (Date) it.get(1));
    assertEquals(Time.valueOf("09:05:10"), (Time) it.get(2));
    assertEquals(Timestamp.valueOf("2018-09-01 09:05:10"), (Timestamp) it.get(3));
    assertTrue(it.next());
    assertEquals("10001", it.get(0));
    assertEquals(Date.valueOf("2018-06-01"), (Date) it.get(1));
    assertEquals(Time.valueOf("10:14:45"), (Time) it.get(2));
    assertEquals(Timestamp.valueOf("2018-06-01 10:14:45"), (Timestamp) it.get(3));
    assertTrue(it.next());
    assertNull(it.get(0));
    assertNull(it.get(1));
    assertNull(it.get(2));
    assertNull(it.get(3));
    assertFalse(it.next());
    result.close();
    // select * from date where cur_date < '2018-09-01'
    Calendar date = Calendar.getInstance();
    date.clear();
    date.set(Calendar.YEAR, 2018);
    date.set(Calendar.MONTH, 8); // Note: calendar month starts with 0
    date.set(Calendar.DAY_OF_MONTH, 1);
    Expression dateCmp = ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN,
        ExpressionFactory.createInputRef(1, ColumnType.DATE, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.DATE, date));
    plan.setWhereExps(ImmutableList.of(dateCmp));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals("10001", it.get(0));
    assertEquals(Date.valueOf("2018-06-01"), (Date) it.get(1));
    assertEquals(Time.valueOf("10:14:45"), (Time) it.get(2));
    assertEquals(Timestamp.valueOf("2018-06-01 10:14:45"), (Timestamp) it.get(3));
    assertFalse(it.next());
    result.close();
    // select * from date where cur_time > time '09:05:10' and time_stamp < timestamp '2018-09-01 09:05:10'
    Calendar time = Calendar.getInstance();
    time.clear();
    time.setTimeZone(TimeZone.getTimeZone("GMT"));
    time.set(Calendar.HOUR_OF_DAY, 10);
    time.set(Calendar.MINUTE, 14);
    time.set(Calendar.SECOND, 45);
    Expression timeCmp = ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN,
        ExpressionFactory.createInputRef(2, ColumnType.TIME, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.TIME, time));
    plan.setWhereExps(ImmutableList.of(timeCmp));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals("10000", it.get(0));
    assertEquals(Date.valueOf("2018-09-01"), (Date) it.get(1));
    assertEquals(Time.valueOf("09:05:10"), (Time) it.get(2));
    assertEquals(Timestamp.valueOf("2018-09-01 09:05:10"), (Timestamp) it.get(3));
    assertFalse(it.next());
    result.close();
    // select * from date where time_stamp >= timestamp '2018-09-01 09:05:10'
    Calendar ts = Calendar.getInstance();
    ts.clear();
    ts.setTimeZone(TimeZone.getTimeZone("GMT"));
    ts.set(Calendar.YEAR, 2018);
    ts.set(Calendar.MONTH, 8);
    ts.set(Calendar.DAY_OF_MONTH, 1);
    ts.set(Calendar.HOUR_OF_DAY, 9);
    ts.set(Calendar.MINUTE, 5);
    ts.set(Calendar.SECOND, 10);
    Expression tsCmp = ExpressionFactory.createBinaryOperator(OperatorType.GE, ColumnType.BOOLEAN,
        ExpressionFactory.createInputRef(3, ColumnType.TIMESTAMP, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.TIMESTAMP, ts));
    plan.setWhereExps(ImmutableList.of(tsCmp));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals("10000", it.get(0));
    assertEquals(Date.valueOf("2018-09-01"), (Date) it.get(1));
    assertEquals(Time.valueOf("09:05:10"), (Time) it.get(2));
    assertEquals(Timestamp.valueOf("2018-09-01 09:05:10"), (Timestamp) it.get(3));
    assertFalse(it.next());
    result.close();
    // select * from traffic
    plan = new LeafPlan();
    plan.setTableName("traffic");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("traffic")));
    result = adapter.query(plan);
    it = result.getIterator();
    assertTrue(it.next());
    assertEquals(GeometryUtils.fromString(String.format("POINT(%d %d)", 0, -1)), it.get(1));
    assertTrue(it.next());
    assertEquals(GeometryUtils.fromString(String.format("POINT(%d %d)", 0, 0)), it.get(1));
    assertTrue(it.next());
    assertEquals(GeometryUtils.fromString(String.format("POINT(%d %d)", 1, -1)), it.get(1));
    assertTrue(it.next());
    assertEquals(GeometryUtils.fromString(String.format("POINT(%d %d)", -1, 1)), it.get(1));
    assertTrue(it.next());
    assertNull(it.get(1));
    assertFalse(it.next());
    result.close();
  }
}
