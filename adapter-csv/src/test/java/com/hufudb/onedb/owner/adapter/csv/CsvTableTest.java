package com.hufudb.onedb.owner.adapter.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;

public class CsvTableTest {
  @Test
  public void testScanWithSchema() throws IOException {
    URL source = CsvTableTest.class.getClassLoader().getResource("data/test2.csv");
    CsvTable table = new CsvTable("test2", Paths.get(source.getPath()));
    Schema.Builder builder = Schema.newBuilder();
    builder.add("Department", ColumnType.STRING, Modifier.PUBLIC);
    builder.add("Name", ColumnType.STRING, Modifier.PUBLIC);
    builder.add("Weight", ColumnType.FLOAT, Modifier.PUBLIC);
    builder.add("Score", ColumnType.INT, Modifier.PUBLIC);
    DataSet result = table.scanWithSchema(builder.build(), ImmutableList.of(3, 0, 4, 2));
    DataSetIterator it = result.getIterator();
    assertTrue(it.next());
    assertEquals("computer", it.get(0));
    assertEquals("tom", it.get(1));
    assertEquals(75.5, ((Number) it.get(2)).doubleValue(), 0.001);
    assertEquals(90, it.get(3));
    assertTrue(it.next());
    assertEquals("anna", it.get(1));
    assertTrue(it.next());
    assertEquals(69.1, ((Number) it.get(2)).doubleValue(), 0.001);
    assertFalse(it.next());
    result.close();
  }
}
