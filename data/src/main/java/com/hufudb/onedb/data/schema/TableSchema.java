package com.hufudb.onedb.data.schema;

import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBData.TableSchemaListProto;
import com.hufudb.onedb.proto.OneDBData.TableSchemaProto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Map table's name to it's {@link Schema}
 * Represents an ACTUAL table's information in {@link SchemaManager}
 * Encapsulation in protocol buffer, Immutable
 */
public class TableSchema {
  protected final String name;
  protected final Schema schema;
  protected final Map<String, Integer> columnIndex;

  TableSchema(String name, Schema schema, Map<String, Integer> columnIndex) {
    this.name = name;
    this.schema = schema;
    this.columnIndex = ImmutableMap.copyOf(columnIndex);
  }

  TableSchema(String name, Schema schema) {
    this.name = name;
    this.schema = schema;
    ImmutableMap.Builder<String, Integer> indexBuilder = ImmutableMap.builder();
    for (int i = 0; i < schema.size(); ++i) {
      indexBuilder.put(schema.getName(i), i);
    }
    this.columnIndex = indexBuilder.build();
  }

  TableSchema(String name) {
    this.name = name;
    this.schema = Schema.EMPTY;
    this.columnIndex = ImmutableMap.of();
  }

  public static TableSchema fromProto(TableSchemaProto proto) {
    return new TableSchema(proto.getName(), Schema.fromProto(proto.getSchema()));
  }

  public static TableSchema fromName(String name) {
    return new TableSchema(name);
  }

  public static List<TableSchema> fromProto(TableSchemaListProto proto) {
    return proto.getTableList().stream()
        .map(info -> TableSchema.fromProto(info))
        .collect(Collectors.toList());
  }

  public static TableSchema of(String name, Schema header) {
    return new TableSchema(name, header);
  }

  public static TableSchema of(String name, List<ColumnDesc> columns) {
    return new TableSchema(name, new Schema(columns));
  }

  public static TableSchema of(String name) {
    return new TableSchema(name);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public TableSchemaProto toProto() {
    return TableSchemaProto.newBuilder().setName(name).setSchema(schema.toProto()).build();
  }

  public String getName() {
    return name;
  }

  public Schema getSchema() {
    return schema;
  }

  public int getColumnIndex(String name) {
    return columnIndex.get(name);
  }

  public int size() {
    return schema.size();
  }

  @Override
  public String toString() {
    return String.format("[%s](%s)", name, schema);
  }

  public static class Builder {
    protected final Schema.Builder builder;
    protected final Map<String, Integer> columnIndex;
    protected String tableName;

    protected Builder() {
      this.builder = Schema.newBuilder();
      columnIndex = new HashMap<>();
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder add(String name, ColumnType type) {
      if (columnIndex.containsKey(name)) {
        throw new RuntimeException("column " + name + " already exist");
      }
      columnIndex.put(name, builder.size());
      builder.add(name, type);
      return this;
    }

    public Builder add(String name, ColumnType type, Modifier modifier) {
      if (columnIndex.containsKey(name)) {
        throw new RuntimeException("column " + name + " already exist");
      }
      columnIndex.put(name, builder.size());
      builder.add(name, type, modifier);
      return this;
    }

    public TableSchema build() {
      return new TableSchema(tableName, builder.build(), columnIndex);
    }
  }
}

