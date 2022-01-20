package com.hufudb.onedb.backend.utils;

import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.table.TableMeta.LocalTableMeta;

public class SimpleGlobalTableInfo {
  String name;
  Header header;
  List<LocalTableMeta> mappings;

  public static SimpleGlobalTableInfo from(OneDBTableInfo info) {
    SimpleGlobalTableInfo sinfo = new SimpleGlobalTableInfo();
    sinfo.setName(info.getName());
    sinfo.setHeader(info.getHeader());
    sinfo.setMappings(info.getMappings());
    return sinfo;
  }

  public static List<SimpleGlobalTableInfo> from(List<OneDBTableInfo> info) {
    List<SimpleGlobalTableInfo> sinfo = new ArrayList<>();
    for (OneDBTableInfo i : info) {
      sinfo.add(SimpleGlobalTableInfo.from(i));
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
