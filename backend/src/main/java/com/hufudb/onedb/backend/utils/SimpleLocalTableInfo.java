package com.hufudb.onedb.backend.utils;

import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.TableInfo;

public class SimpleLocalTableInfo {
  private String name;
  private Header header;

  public static SimpleLocalTableInfo from(TableInfo info) {
    SimpleLocalTableInfo sinfo = new SimpleLocalTableInfo();
    sinfo.setName(info.getName());
    sinfo.setHeader(info.getHeader());
    return sinfo;
  }

  public static List<SimpleLocalTableInfo> from(List<TableInfo> info) {
    List<SimpleLocalTableInfo> sinfo = new ArrayList<>();
    for (TableInfo i : info) {
      SimpleLocalTableInfo si = new SimpleLocalTableInfo();
      si.setName(i.getName());
      si.setHeader(i.getHeader());
      sinfo.add(si);
    }
    return sinfo;
  }

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public Header getHeader() {
    return header;
  }
  public void setHeader(Header header) {
    this.header = header;
  }
}
