package com.hufudb.onedb.core.data.utils;

import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.TableInfo;

public class POJOLocalTableInfo {
  private String name;
  private POJOHeader header;

  public static POJOLocalTableInfo from(TableInfo info) {
    POJOLocalTableInfo sinfo = new POJOLocalTableInfo();
    sinfo.setName(info.getName());
    sinfo.setHeader(POJOHeader.fromHeader(info.getHeader()));
    return sinfo;
  }

  public static List<POJOLocalTableInfo> from(List<TableInfo> info) {
    List<POJOLocalTableInfo> sinfo = new ArrayList<>();
    for (TableInfo i : info) {
      POJOLocalTableInfo si = new POJOLocalTableInfo();
      si.setName(i.getName());
      si.setHeader(POJOHeader.fromHeader(i.getHeader()));
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
  public POJOHeader getHeader() {
    return header;
  }
  public void setHeader(POJOHeader header) {
    this.header = header;
  }
}
