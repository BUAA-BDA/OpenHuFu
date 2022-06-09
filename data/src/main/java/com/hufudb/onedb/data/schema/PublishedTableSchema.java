package com.hufudb.onedb.data.schema;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.Modifier;

/**
 * Used for schema mapping, see @SchemaManager.java for detail
 */
public class PublishedTableSchema {
  private final TableSchema fakeSchema;
  private final TableSchema actualSchema;
  private final List<Integer> mappings;

  public PublishedTableSchema(
      TableSchema tableSchema,
      String publishedTableName,
      List<ColumnDesc> publishedColumns,
      List<Integer> mappings) {
    this.actualSchema = tableSchema;
    if (publishedColumns.isEmpty()) {
      this.fakeSchema = TableSchema.of(publishedTableName, tableSchema.getSchema());
      ImmutableList.Builder<Integer> mapBuilder =
          ImmutableList.builderWithExpectedSize(tableSchema.size());
      for (int i = 0; i < tableSchema.size(); ++i) {
        mapBuilder.add(i);
      }
      this.mappings = mapBuilder.build();
    } else {
      // todo: check unique of mapping
      assert mappings.size() == publishedColumns.size();
      ImmutableList.Builder<ColumnDesc> columnBuilder = ImmutableList.builder();
      ImmutableList.Builder<Integer> mapBuilder = ImmutableList.builder();
      for (int i = 0; i < publishedColumns.size(); ++i) {
        if (!publishedColumns.get(i).getModifier().equals(Modifier.HIDDEN)) {
          columnBuilder.add(publishedColumns.get(i));
          mapBuilder.add(mappings.get(i));
        }
      }
      this.fakeSchema = TableSchema.of(publishedTableName, columnBuilder.build());
      this.mappings = ImmutableList.copyOf(mapBuilder.build());
    }
  }

  public static PublishedTableSchema create(TableSchema schema, String publishedTableName) {
    return new PublishedTableSchema(schema, publishedTableName, ImmutableList.of(), ImmutableList.of());
  }

  public String getActualTableName() {
    return actualSchema.getName();
  }

  public String getPublishedTableName() {
    return fakeSchema.getName();
  }

  public TableSchema getFakeTableSchema() {
    return fakeSchema;
  }

  public Schema getPublishedSchema() {
    return fakeSchema.getSchema();
  }

  public List<Integer> getMappings() {
    return ImmutableList.copyOf(mappings);
  }

  @Override
  public String toString() {
    return String.format("%s -> %s", fakeSchema.toString(), actualSchema.toString());
  }
}
