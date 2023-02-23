package com.hufudb.openhufu.data.schema.utils;

import java.util.List;

public class PojoTableSchema {
  public String name;
  public List<PojoColumnDesc> columns;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<PojoColumnDesc> getColumns() {
    return columns;
  }

  public void setColumns(List<PojoColumnDesc> columns) {
    this.columns = columns;
  }
}
