package com.hufudb.onedb.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class ProtoDataSetTest {

  List<Row> generateRandomRow(int size) {
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

  public void compare(List<Row> rows, DataSetIterator it) {
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
  public void testProtoDataSetSmoke() {
    Schema schema = Schema.newBuilder()
        .add("A", ColumnType.BOOLEAN, Modifier.HIDDEN)
        .add("B", ColumnType.BYTE, Modifier.PUBLIC)
        .add("C", ColumnType.INT, Modifier.PORTECTED)
        .add("D", ColumnType.LONG, Modifier.PRIVATE)
        .add("E", ColumnType.STRING, Modifier.PUBLIC)
        .add("F", ColumnType.BLOB, Modifier.PUBLIC).build();
    assertEquals("A", schema.getName(0));
    assertEquals(ColumnType.BYTE, schema.getType(1));
    assertEquals(Modifier.PORTECTED, schema.getModifier(2));
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    List<Row> rows = generateRandomRow(10);
    for (Row row : rows) {
      dBuilder.addRow(row);
    }
    DataSetIterator it = dBuilder.build().getIterator();
    compare(rows, it);
    dBuilder.clear();
    rows = generateRandomRow(10);
    for (Row row : rows) {
      dBuilder.addRow(row);
    }
    it = dBuilder.build().getIterator();
    compare(rows, it);
  }
}
