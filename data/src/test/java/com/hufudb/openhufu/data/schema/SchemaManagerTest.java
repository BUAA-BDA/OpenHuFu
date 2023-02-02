package com.hufudb.openhufu.data.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.hufudb.openhufu.data.schema.PublishedTableSchema;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.schema.TableSchema;
import org.junit.Test;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;

public class SchemaManagerTest {
  @Test
  public void schemaManagerTest() {
    SchemaManager manager = new SchemaManager();
    TableSchema.Builder builder1 = TableSchema.newBuilder();
    TableSchema.Builder builderExpect = TableSchema.newBuilder();
    builder1
    .setTableName("T1")
    .add("A", ColumnType.BOOLEAN, Modifier.PUBLIC)
    .add("B", ColumnType.FLOAT)
    .add("C", ColumnType.DOUBLE)
    .add("D", ColumnType.INT)
    .add("E", ColumnType.LONG)
    .add("F", ColumnType.STRING)
    .add("G", ColumnType.BLOB);
    builderExpect.setTableName("PT1")
    .add("A", ColumnType.BOOLEAN, Modifier.PUBLIC)
    .add("B", ColumnType.FLOAT, Modifier.PUBLIC)
    .add("C", ColumnType.DOUBLE, Modifier.PUBLIC)
    .add("D", ColumnType.INT, Modifier.PUBLIC)
    .add("E", ColumnType.LONG, Modifier.PUBLIC)
    .add("F", ColumnType.STRING, Modifier.PUBLIC)
    .add("G", ColumnType.BLOB, Modifier.PUBLIC);
    TableSchema expectSchema = builderExpect.build();
    try {
      builder1.add("A", ColumnType.SHORT);
      assertFalse("error when adding an existing column", true);
    } catch (RuntimeException e) {}
    try {
      builder1.add("B", ColumnType.LONG, Modifier.PRIVATE);
      assertFalse("error when adding an existing column", true);
    } catch (RuntimeException e) {}
    manager.addLocalTable(builder1.build());
    manager.addLocalTable(builder1.build());
    TableSchema t1 = manager.getLocalTable("T1");
    assertEquals("T1", t1.getName());
    assertEquals(7, t1.getSchema().size());
    assertEquals(1, manager.getAllLocalTable().size());
    PublishedTableSchema pt1 = PublishedTableSchema.create(t1, "PT1");
    assertEquals(7, pt1.getMappings().size());
    assertEquals(expectSchema.getSchema(), pt1.getPublishedSchema());
    assertTrue(manager.addPublishedTable(pt1));
    assertFalse(manager.addPublishedTable(pt1));
    assertEquals("T1", manager.getActualTableName("PT1"));
    assertEquals("", manager.getActualTableName("PT2"));
    assertEquals(1, manager.getAllPublishedTable().size());
    manager.clearPublishedTable();
    assertEquals(Schema.EMPTY, manager.getPublishedSchema("PT1"));
    manager.dropPublishedTable("PT1");
    manager.addPublishedTable(PublishedTableSchema.create(t1, "PT1"));
    manager.dropPublishedTable("PT1");
    assertEquals(Schema.EMPTY, manager.getPublishedSchema("PT1"));
  }
}
