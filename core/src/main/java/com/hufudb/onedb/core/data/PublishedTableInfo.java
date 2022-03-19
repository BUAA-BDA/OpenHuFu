package com.hufudb.onedb.core.data;

import java.util.List;

public class PublishedTableInfo {

  private String originTableName;
  private TableInfo fakeTableInfo;

  public PublishedTableInfo(TableInfo tableInfo, String aliasTableName, List<Field> aliasFields) {
    this.originTableName = tableInfo.getName();
    if (aliasFields == null || aliasFields.isEmpty()) {
      aliasFields = tableInfo.getHeader().getFields();
    }
    VirtualHeader header = VirtualHeader.of(tableInfo.getHeader(), aliasFields);
    this.fakeTableInfo = new TableInfo(aliasTableName, header.getAliasHeader());
  }

  public String getOriginTableName() {
    return originTableName;
  }

  public String getPublishedTableName() {
    return fakeTableInfo.getName();
  }

  public TableInfo getFakeTableInfo() {
    return fakeTableInfo;
  }

  @Override
  public String toString() {
      return fakeTableInfo.toString();
  }
}
