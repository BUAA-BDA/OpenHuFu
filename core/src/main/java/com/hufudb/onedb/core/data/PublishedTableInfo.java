package com.hufudb.onedb.core.data;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class PublishedTableInfo {

  private TableInfo fakeTableInfo;
  private TableInfo originTableInfo;
  private List<Integer> mappings;

  public PublishedTableInfo(TableInfo tableInfo, String publishedTableName, List<Field> publishedFields, List<Integer> mappings) {
    this.originTableInfo = tableInfo;
    if (publishedFields == null || publishedFields.isEmpty()) {
      this.fakeTableInfo = TableInfo.of(publishedTableName, tableInfo.getHeader());
      ImmutableList.Builder<Integer> mapBuilder = ImmutableList.builderWithExpectedSize(publishedFields.size());
      for (int i = 0; i < publishedFields.size(); ++i) {
        mapBuilder.add(i);
      }
      this.mappings = mapBuilder.build();
    } else {
      // todo: check publishedFields.length == mappings.length
      // todo: check unique of mapping
      ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
      ImmutableList.Builder<Integer> mapBuilder = ImmutableList.builder();
      for (int i = 0 ; i < publishedFields.size(); ++i) {
        if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
          fieldBuilder.add(publishedFields.get(i));
          mapBuilder.add(mappings.get(i));
        }
      }
      this.fakeTableInfo = TableInfo.of(publishedTableName, fieldBuilder.build());
      this.mappings = ImmutableList.copyOf(mapBuilder.build());
    }
  }

  public PublishedTableInfo(TableInfo tableInfo, String publishedTableName) {
    this(tableInfo, publishedTableName, ImmutableList.of(), ImmutableList.of());
  }

  public String getOriginTableName() {
    return originTableInfo.getName();
  }

  public String getPublishedTableName() {
    return fakeTableInfo.getName();
  }

  public TableInfo getFakeTableInfo() {
    return fakeTableInfo;
  }

  public List<String> getOriginNames() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (int i = 0; i < originTableInfo.header.size(); ++i) {
      builder.add(originTableInfo.header.getName(mappings.get(i)));
    }
    return builder.build();
  }

  public Field getOriginField(int index) {
    // todo: check index out of range
    return originTableInfo.header.getField(mappings.get(index));
  }

  public List<Integer> getMappings() {
    return ImmutableList.copyOf(mappings);
  }

  @Override
  public String toString() {
      return String.format("%s -> %s", fakeTableInfo.toString(), originTableInfo.toString());
  }
}
