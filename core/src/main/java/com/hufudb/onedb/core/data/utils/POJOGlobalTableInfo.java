package com.hufudb.onedb.core.data.utils;

import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.table.TableMeta.LocalTableMeta;

public class POJOGlobalTableInfo {
  String name;
  POJOHeader header;
  List<LocalTableMeta> mappings;

  public static POJOGlobalTableInfo from(OneDBTableInfo info) {
    POJOGlobalTableInfo sinfo = new POJOGlobalTableInfo();
    sinfo.setName(info.getName());
    sinfo.setHeader(POJOHeader.fromHeader(info.getHeader()));
    sinfo.setMappings(info.getMappings());
    return sinfo;
  }

  public static List<POJOGlobalTableInfo> from(List<OneDBTableInfo> info) {
    List<POJOGlobalTableInfo> sinfo = new ArrayList<>();
    for (OneDBTableInfo i : info) {
      sinfo.add(POJOGlobalTableInfo.from(i));
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

  public List<LocalTableMeta> getMappings() {
    return mappings;
  }

  public void setMappings(List<LocalTableMeta> mappings) {
    this.mappings = mappings;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
