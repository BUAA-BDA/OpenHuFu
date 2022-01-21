package com.hufudb.onedb.core.data;

import java.util.List;

public class VirtualTableInfo {

  private TableInfo originTableInfo;
  private String virtualTableName;
  private TableInfo fakeTableInfo;

  public VirtualTableInfo(TableInfo tableInfo, String aliasTableName, List<Field> aliasFields) {
    this.originTableInfo = tableInfo;
    this.virtualTableName = aliasTableName;
    if (aliasFields == null || aliasFields.isEmpty()) {
      aliasFields = tableInfo.getHeader().getFields();
    }
    VirtualHeader header = VirtualHeader.of(tableInfo.getHeader(), aliasFields);
    this.fakeTableInfo = new TableInfo(virtualTableName, header.getAliasHeader());
  }

  public TableInfo getOriginTableInfo() {
    return originTableInfo;
  }

  public TableInfo getFakeTableInfo() {
    return fakeTableInfo;
  }

  public String getVirtualTableName() {
    return virtualTableName;
  }

  @Override
  public String toString() {
      return fakeTableInfo.toString();
  }
}
