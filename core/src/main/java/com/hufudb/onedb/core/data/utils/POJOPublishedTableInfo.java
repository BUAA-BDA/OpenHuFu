package com.hufudb.onedb.core.data.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.PublishedTableInfo;

public class POJOPublishedTableInfo {
  public String publishedTableName;
  public String originTableName;
  public List<Field> publishedFields;
  public List<String> originNames;

  public POJOPublishedTableInfo(String publishedTableName, String originTableName, List<Field> publishedFields,
      List<String> originNames) {
    this.publishedTableName = publishedTableName;
    this.originTableName = originTableName;
    this.publishedFields = publishedFields;
    this.originNames = originNames;
  }

  public static POJOPublishedTableInfo from(PublishedTableInfo info) {
    return new POJOPublishedTableInfo(info.getPublishedTableName(), info.getOriginTableName(),
        ImmutableList.copyOf(info.getFakeTableInfo().getHeader().getFields()), info.getOriginNames());
  }

  // public PublishedTableInfo to() {
  //   List<Field> pFields = new ArrayList<>();
  //   List<Integer> mappings = new ArrayList<>();
  //   for (int i = 0; i < publishedFields.size(); ++i) {
  //     if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
  //       pFields.add(publishedFields.get(i));
  //       mappings.add(null);
  //     }
  //   }
  // }

  public static List<POJOPublishedTableInfo> from(List<PublishedTableInfo> tableInfos) {
    return tableInfos.stream().map(info -> from(info)).collect(Collectors.toList());
  }

  public String getPublishedTableName() {
    return publishedTableName;
  }

  public void setPublishedTableName(String publishedTableName) {
    this.publishedTableName = publishedTableName;
  }

  public String getOriginTableName() {
    return originTableName;
  }

  public void setOriginTableName(String originTableName) {
    this.originTableName = originTableName;
  }

  public List<Field> getPublishedFields() {
    return publishedFields;
  }

  public void setPublishedFields(List<Field> publishedFields) {
    this.publishedFields = publishedFields;
  }

  public List<String> getOriginNames() {
    return originNames;
  }

  public void setOriginNames(List<String> originNames) {
    this.originNames = originNames;
  }
}
