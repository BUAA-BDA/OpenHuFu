package com.hufudb.onedb.core.data;

import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableInfoProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableInfo {

  protected String name;
  protected Header header;

  protected Map<String, Integer> columnIndex;

  TableInfo(String name, Header header, Map<String, Integer> columnIndex) {
    this.name = name;
    this.header = header;
    this.columnIndex = columnIndex;
  }

  TableInfo(String name, Header header) {
    this.name = name;
    this.header = header.immutableCopy();
    this.columnIndex = new HashMap<>();
    for (int i = 0; i < header.size(); ++i) {
      if (columnIndex.containsKey(name)) {
        throw new RuntimeException("column " + name + " already exist");
      }
      columnIndex.put(header.getName(i), i);
    }
  }

  TableInfo(String name) {
    this.name = name;
    this.header = Header.EMPTY;
    this.columnIndex = ImmutableMap.of();
  }

  public static TableInfo fromProto(LocalTableInfoProto proto) {
    return new TableInfo(proto.getName(), Header.fromProto(proto.getHeader()));
  }

  public static List<TableInfo> fromProto(LocalTableListProto proto) {
    return proto.getTableList().stream()
        .map(info -> TableInfo.fromProto(info))
        .collect(Collectors.toList());
  }

  public static TableInfo of(String name, Header header) {
    return new TableInfo(name, header);
  }

  public static TableInfo of(String name, List<Field> fields) {
    return new TableInfo(name, new Header(fields));
  }

  public static TableInfo of(String name) {
    return new TableInfo(name);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public LocalTableInfoProto toProto() {
    return LocalTableInfoProto.newBuilder().setName(name).setHeader(header.toProto()).build();
  }

  public String getName() {
    return name;
  }

  public Header getHeader() {
    return header;
  }

  public Integer getColumnIndex(String name) {
    return columnIndex.get(name);
  }

  @Override
  public String toString() {
    return String.format("[%s](%s)", name, header);
  }

  public static class Builder {
    protected final Header.Builder builder;
    protected final Map<String, Integer> columnIndex;
    protected String tableName;

    protected Builder() {
      this.builder = Header.newBuilder();
      columnIndex = new HashMap<>();
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder add(String name, FieldType type) {
      if (columnIndex.containsKey(name)) {
        throw new RuntimeException("column " + name + " already exist");
      }
      columnIndex.put(name, builder.size());
      builder.add(name, type);
      return this;
    }

    public Builder add(String name, FieldType type, Level level) {
      if (columnIndex.containsKey(name)) {
        throw new RuntimeException("column " + name + " already exist");
      }
      columnIndex.put(name, builder.size());
      builder.add(name, type, level);
      return this;
    }

    public TableInfo build() {
      return new TableInfo(tableName, builder.build(), columnIndex);
    }
  }
}
