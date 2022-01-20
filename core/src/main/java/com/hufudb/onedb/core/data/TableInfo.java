package com.hufudb.onedb.core.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hufudb.onedb.rpc.OneDBCommon.LocalTableInfoProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;

public class TableInfo {
  private final String name;
  private final Header header;

  private final Map<String, Integer> columnIndex;

  private TableInfo(String name, Header header, Map<String, Integer> columnIndex) {
    this.name = name;
    this.header = header;
    this.columnIndex = columnIndex;
  }

  private TableInfo(String name, Header header) {
    this.name = name;
    this.header = header;
    this.columnIndex = new HashMap<>();
    for (int i = 0; i < header.size(); ++i) {
      columnIndex.put(header.getName(i), i);
    }
  }

  public LocalTableInfoProto toProto() {
    return LocalTableInfoProto.newBuilder().setName(name).setHeader(header.toProto()).build();
  }

  public static TableInfo fromProto(LocalTableInfoProto proto) {
    return new TableInfo(proto.getName(), Header.fromProto(proto.getHeader()));
  }

  public static List<TableInfo> fromProto(LocalTableListProto proto) {
    return proto.getTableList().stream()
        .map(info -> TableInfo.fromProto(info)).collect(Collectors.toList());
  }

  public static TableInfo of(String name, Header header) {
    return new TableInfo(name, header);
  }

  public static class Builder {
    private String tableName;
    private final Header.Builder builder;
    private final Map<String, Integer> columnIndex;

    private Builder() {
      this.builder = Header.newBuilder();
      columnIndex = new HashMap<>();
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder add(String name, FieldType type) {
      columnIndex.put(name, builder.size());
      builder.add(name, type);
      return this;
    }

    public Builder add(String name, FieldType type, Level level) {
      columnIndex.put(name, builder.size());
      builder.add(name, type, level);
      return this;
    }

    public TableInfo build() {
      return new TableInfo(tableName, builder.build(), columnIndex);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
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
}
