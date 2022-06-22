package com.hufudb.onedb.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.MultiSourceDataSet.Producer;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Direction;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class DataSetTest {

  List<Row> generateRandomRowProto(int size) {
    List<Row> rows = new ArrayList<>();
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < size; ++i) {
      ArrayRow.Builder builder = ArrayRow.newBuilder(6);
      builder.set(0, random.nextBoolean());
      builder.set(1, (byte) random.nextInt());
      builder.set(2, random.nextInt());
      builder.set(3, random.nextLong());
      builder.set(4, RandomStringUtils.randomAlphabetic(20));
      builder.set(5, RandomStringUtils.randomAscii(20).getBytes());
      rows.add(builder.build());
    }
    return rows;
  }

  public void compareProto(List<Row> rows, DataSetIterator it) {
    int i = 0;
    while (it.next()) {
      assertEquals(rows.get(i).get(0), it.get(0));
      assertEquals(((Number) rows.get(i).get(1)).intValue(), (int) it.get(1));
      assertEquals((Number) rows.get(i).get(2), (int) it.get(2));
      assertEquals((long) rows.get(i).get(3), (long) it.get(3));
      assertEquals((String) rows.get(i).get(4), (String) it.get(4));
      assertArrayEquals((byte[]) rows.get(i).get(5), (byte[]) it.get(5));
      i++;
    }
  }

  @Test
  public void testProtoDataSet() {
    Schema schema = Schema.newBuilder().add("A", ColumnType.BOOLEAN, Modifier.HIDDEN)
        .add("B", ColumnType.BYTE, Modifier.PUBLIC).add("C", ColumnType.INT, Modifier.PROTECTED)
        .add("D", ColumnType.LONG, Modifier.PRIVATE).add("E", ColumnType.STRING, Modifier.PUBLIC)
        .add("F", ColumnType.BLOB, Modifier.PUBLIC).build();
    assertEquals("A", schema.getName(0));
    assertEquals(ColumnType.BYTE, schema.getType(1));
    assertEquals(Modifier.PROTECTED, schema.getModifier(2));
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    List<Row> rows = generateRandomRowProto(10);
    for (Row row : rows) {
      dBuilder.addRow(row);
    }
    DataSetIterator it = dBuilder.build().getIterator();
    compareProto(rows, it);
    dBuilder.clear();
    rows = generateRandomRowProto(10);
    for (Row row : rows) {
      dBuilder.addRow(row);
    }
    it = dBuilder.build().getIterator();
    compareProto(rows, it);
  }

  @Test
  public void testProtoRowDataSet() {
    Schema schema = Schema.newBuilder().add("A", ColumnType.BOOLEAN, Modifier.HIDDEN)
        .add("B", ColumnType.BYTE, Modifier.PUBLIC).add("C", ColumnType.INT, Modifier.PROTECTED)
        .add("D", ColumnType.LONG, Modifier.PRIVATE).add("E", ColumnType.STRING, Modifier.PUBLIC)
        .add("F", ColumnType.BLOB, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    List<Row> rows = generateRandomRowProto(10);
    for (Row row : rows) {
      dBuilder.addRow(row);
    }
    ProtoDataSet source = dBuilder.build();
    ProtoRowDataSet rowDataSet = ProtoRowDataSet.materialize(source);
    ProtoRowDataSet projectDataSet = ProtoRowDataSet.project(source, ImmutableList.of(0, 2));
    assertEquals(source.getSchema(), rowDataSet.getSchema());
    assertEquals(source.rowCount(), rowDataSet.rowCount());
    assertEquals(source.rowCount(), projectDataSet.rowCount());
    DataSetIterator vit = source.getIterator();
    DataSetIterator rit = rowDataSet.getIterator();
    DataSetIterator pit = projectDataSet.getIterator();
    while (vit.next() && rit.next() && pit.next()) {
      assertEquals(vit.get(0), rit.get(0));
      assertEquals(((Number) vit.get(1)).intValue(), (int) rit.get(1));
      assertEquals((Number) vit.get(2), (int) rit.get(2));
      assertEquals((long) vit.get(3), (long) rit.get(3));
      assertEquals((String) vit.get(4), (String) rit.get(4));
      assertArrayEquals((byte[]) vit.get(5), (byte[]) rit.get(5));
      assertEquals(vit.get(0), pit.get(0));
      assertEquals((Number) vit.get(2), (int) pit.get(1));
    }
    projectDataSet.close();
  }

  DataSetProto generateDataSet(Schema schema, int size) {
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < size; ++i) {
      ArrayRow.Builder builder = ArrayRow.newBuilder(2);
      builder.set(0, random.nextInt());
      builder.set(1, random.nextLong());
      dBuilder.addRow(builder.build());
    }
    return dBuilder.buildProto();
  }

  @Test
  public void testMultiSourceDataSet() {
    for (int i = 0; i < 20; ++i) {
      final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
          .add("B", ColumnType.LONG, Modifier.PUBLIC).build();
      MultiSourceDataSet multi = new MultiSourceDataSet(schema, 2, 1000);
      ExecutorService threadPool = Executors.newFixedThreadPool(2);
      final Producer p1 = multi.newProducer();
      final Producer p2 = multi.newProducer();
      final Random random = new Random();
      threadPool.submit(new Runnable() {
        @Override
        public void run() {
          p1.add(generateDataSet(schema, 10));
          try {
            Thread.sleep(random.nextInt(100));
          } catch (Exception e) {
            e.printStackTrace();
          }
          p1.add(generateDataSet(schema, 12));
          p1.finish();
        }
      });
      threadPool.submit(new Runnable() {
        @Override
        public void run() {
          p2.add(generateDataSet(schema, 11));
          try {
            Thread.sleep(random.nextInt(100));
          } catch (Exception e) {
            e.printStackTrace();
          }
          p2.add(generateDataSet(schema, 13));
          p2.finish();
        }
      });
      DataSetIterator it = multi.getIterator();
      int count = 0;
      while (it.next()) {
        count++;
      }
      assertEquals(46, count);
      multi.close();
    }
  }

  @Test
  public void testHorizontalDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.LONG, Modifier.PUBLIC).build();
    ProtoDataSet d0 = ProtoDataSet.create(generateDataSet(schema, 3));
    ProtoDataSet d1 = ProtoDataSet.create(generateDataSet(schema, 2));
    ProtoDataSet d2 = ProtoDataSet.create(generateDataSet(schema, 5));
    HorizontalDataSet r = new HorizontalDataSet(ImmutableList.of(d0, d1, d2));
    assertEquals(r.rowCount(), 10);
    DataSetIterator it = r.getIterator();
    DataSetIterator it0 = d0.getIterator();
    DataSetIterator it1 = d1.getIterator();
    DataSetIterator it2 = d2.getIterator();
    while (it0.next()) {
      assertTrue(it.next());
      assertEquals(it0.get(0), it.get(0));
    }
    while (it1.next()) {
      assertTrue(it.next());
      assertEquals(it1.get(0), it.get(0));
    }
    while (it2.next()) {
      assertTrue(it.next());
      assertEquals(it2.get(0), it.get(0));
    }
    r.close();
  }

  @Test
  public void testVerticalDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.LONG, Modifier.PUBLIC).build();
    ProtoDataSet left = ProtoDataSet.create(generateDataSet(schema, 2));
    ProtoDataSet right = ProtoDataSet.create(generateDataSet(schema, 2));
    VerticalDataSet r = VerticalDataSet.create(left, right);
    assertEquals(r.rowCount(), 2);
    DataSetIterator it = r.getIterator();
    DataSetIterator leftIt = left.getIterator();
    DataSetIterator rightIt = right.getIterator();
    while (it.next()) {
      leftIt.next();
      rightIt.next();
      assertEquals(leftIt.get(0), it.get(0));
      assertEquals(rightIt.get(0), it.get(2));
    }
    r.close();
  }

  ProtoDataSet generateUnsortedDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(2);
    builder.reset();
    builder.set(0, 3);
    builder.set(1, 4.2);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 2);
    builder.set(1, 5.3);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 7);
    builder.set(1, 1.4);
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

  @Test
  public void testSortDataSet() {
    Collation c1 = Collation.newBuilder().setDirection(Direction.ASC).setRef(0).build();
    Collation c2 = Collation.newBuilder().setDirection(Direction.DESC).setRef(1).build();
    DataSet source = generateUnsortedDataSet();
    DataSet d1 = SortedDataSet.sort(source, ImmutableList.of(c1));
    DataSetIterator it1 = d1.getIterator();
    assertTrue(it1.next());
    assertEquals(2, it1.get(0));
    assertTrue(it1.next());
    assertEquals(3, it1.get(0));
    assertTrue(it1.next());
    assertEquals(7, it1.get(0));
    assertFalse(it1.next());
    DataSet d2 = SortedDataSet.sort(source, ImmutableList.of(c2));
    DataSetIterator it2 = d2.getIterator();
    assertTrue(it2.next());
    assertEquals(5.3, (double) it2.get(1), 0.001);
    assertTrue(it2.next());
    assertEquals(4.2, (double) it2.get(1), 0.001);
    assertTrue(it2.next());
    assertEquals(1.4, (double) it2.get(1), 0.001);
    assertFalse(it2.next());
    d1.close();
    d2.close();
  }

  @Test
  public void testLimitDataSet() {
    DataSet source = generateUnsortedDataSet();
    DataSet l1 = LimitDataSet.limit(source, 0, 0);
    DataSetIterator itl1 = l1.getIterator();
    assertTrue(itl1.next());
    assertEquals(3, itl1.get(0));
    assertTrue(itl1.next());
    assertEquals(2, itl1.get(0));
    assertTrue(itl1.next());
    assertEquals(7, itl1.get(0));
    assertFalse(itl1.next());
    DataSet l2 = LimitDataSet.limit(source, 1, 1);
    DataSetIterator itl2 = l2.getIterator();
    assertTrue(itl2.next());
    assertEquals(2, itl2.get(0));
    assertFalse(itl2.next());
    DataSet l3 = LimitDataSet.limit(source, 1, 0);
    DataSetIterator itl3 = l3.getIterator();
    assertTrue(itl3.next());
    assertEquals(2, itl3.get(0));
    assertTrue(itl3.next());
    assertEquals(7, itl3.get(0));
    assertFalse(itl3.next());
    DataSet l4 = LimitDataSet.limit(source, 0, 1);
    DataSetIterator itl4 = l4.getIterator();
    assertTrue(itl4.next());
    assertEquals(3, itl4.get(0));
    assertFalse(itl4.next());
    l1.close();
    l2.close();
    l3.close();
    l4.close();
  }

  private List<ArrayRow> generateArrayRows(boolean hasNull, int rowNum) {
    Random random = new Random();
    List<ArrayRow> rows = new ArrayList<>();
    for (int i = 0; i < rowNum; ++i) {
      ArrayRow.Builder builder = ArrayRow.newBuilder(8);
      if (hasNull && random.nextBoolean()) {
        builder.set(0, null);
      } else {
        builder.set(0, random.nextBoolean());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(1, null);
      } else {
        builder.set(1, (byte) random.nextInt());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(2, null);
      } else {
        builder.set(2, random.nextInt());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(3, null);
      } else {
        builder.set(3, random.nextLong());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(4, null);
      } else {
        builder.set(4, random.nextFloat());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(5, null);
      } else {
        builder.set(5, random.nextDouble());
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(6, null);
      } else {
        builder.set(6, RandomStringUtils.randomAlphabetic(20));
      }
      if (hasNull && random.nextBoolean()) {
        builder.set(7, null);
      } else {
        builder.set(7, RandomStringUtils.randomAscii(20).getBytes());
      }
      rows.add(builder.build());
    }
    return rows;
  }

  private DataSet generateMockDataSet(boolean hasNull) {
    Schema schema = Schema.newBuilder().add("A", ColumnType.BOOLEAN, Modifier.HIDDEN)
        .add("B", ColumnType.BYTE, Modifier.PUBLIC).add("C", ColumnType.INT, Modifier.PROTECTED)
        .add("D", ColumnType.LONG, Modifier.PRIVATE).add("E", ColumnType.FLOAT, Modifier.PROTECTED)
        .add("F", ColumnType.DOUBLE, Modifier.PRIVATE).add("G", ColumnType.STRING, Modifier.PUBLIC)
        .add("H", ColumnType.BLOB, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    List<ArrayRow> rows = generateArrayRows(hasNull, 3);
    for (ArrayRow row : rows) {
      dBuilder.addRow(row);
    }
    return dBuilder.build();
  }

  @Test
  public void testResultDataSet() throws SQLException {
    DataSet source = generateMockDataSet(false);
    DataSetIterator exp = source.getIterator();
    MockResultSet mock = new MockResultSet(source);
    ResultDataSet result = new ResultDataSet(source.getSchema(), mock);
    DataSetIterator it = result.getIterator();
    assertEquals(source.getSchema(), result.getSchema());
    while (exp.next()) {
      assertTrue(it.next());
      assertEquals(exp.size(), it.size());
      for (int i = 0; i < 7; ++i) {
        assertEquals(exp.get(i), it.get(i));
      }
      assertArrayEquals((byte[]) exp.get(7), (byte[]) it.get(7));
    }
    assertFalse(it.next());
    result.close();
  }

  private static boolean equals(Row row1, Row row2) {
    if (row1.size() != row2.size()) {
      return false;
    }
    for (int i = 0; i < row1.size(); ++i) {
      Object o1 = row1.get(i);
      Object o2 = row2.get(i);
      if (o1 == null) {
        return o2 == null;
      } else if (o1 instanceof Long || o1 instanceof Integer || o1 instanceof Short || o1 instanceof Byte) {
        if (((Number) o1).longValue() != ((Number) o2).longValue()) {
          return false;
        }
      } else if (o1 instanceof Float || o1 instanceof Double) {
        if (((Number) o1).doubleValue() != ((Number) o2).doubleValue()) {
          return false;
        }
      } else if (o1 instanceof byte[]) {
        return Arrays.equals((byte[]) o1, (byte[]) o2);
      } else if (!o1.equals(o2)){
        return false;
      }
    }
    return true;
  }

  @Test
  public void testDataSetWithNull() {
    List<ArrayRow> rows = generateArrayRows(true, 20);
    Schema schema = Schema.newBuilder().add("A", ColumnType.BOOLEAN, Modifier.HIDDEN)
        .add("B", ColumnType.BYTE, Modifier.PUBLIC).add("C", ColumnType.INT, Modifier.PROTECTED)
        .add("D", ColumnType.LONG, Modifier.PRIVATE).add("E", ColumnType.FLOAT, Modifier.PROTECTED)
        .add("F", ColumnType.DOUBLE, Modifier.PRIVATE).add("G", ColumnType.STRING, Modifier.PUBLIC)
        .add("H", ColumnType.BLOB, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    for (ArrayRow row : rows) {
      dBuilder.addRow(row);
    }
    MaterializedDataSet dataset = dBuilder.build();
    assertEquals(rows.size(), dataset.rowCount());
    DataSetIterator it = dataset.getIterator();
    int k = 0;
    while (it.next()) {
      assertEquals(schema.size(), it.size());
      assertTrue(equals(rows.get(k), it));
      ++k;
    }
  }
}
