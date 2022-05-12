package com.hufudb.onedb.core.data.utils;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.PublishedTableInfo;
import java.util.List;
import java.util.stream.Collectors;

public class POJOPublishedTableInfo {
  public String publishedName;
  public String originName;
  public List<Field> publishedFields;
  public List<Integer> originColumns;

  public POJOPublishedTableInfo(
      String publishedTableName,
      String originTableName,
      List<Field> publishedFields,
      List<Integer> originColumns) {
    this.publishedName = publishedTableName;
    this.originName = originTableName;
    this.publishedFields = publishedFields;
    this.originColumns = originColumns;
  }

  public static POJOPublishedTableInfo from(PublishedTableInfo info) {
    return new POJOPublishedTableInfo(
        info.getPublishedTableName(),
        info.getOriginTableName(),
        ImmutableList.copyOf(info.getFakeTableInfo().getHeader().getFields()),
        info.getMappings());
  }

  public static List<POJOPublishedTableInfo> from(List<PublishedTableInfo> tableSchemas) {
    return tableSchemas.stream().map(info -> from(info)).collect(Collectors.toList());
  }

  public String getPublishedTableName() {
    return publishedName;
  }

  public void setPublishedTableName(String publishedTableName) {
    this.publishedName = publishedTableName;
  }

  public String getOriginTableName() {
    return originName;
  }

  public void setOriginTableName(String originTableName) {
    this.originName = originTableName;
  }

  public List<Field> getPublishedFields() {
    return publishedFields;
  }

  public void setPublishedFields(List<Field> publishedFields) {
    this.publishedFields = publishedFields;
  }

  public List<Integer> getOriginColumns() {
    return originColumns;
  }

  public void setOriginColumns(List<Integer> originColumns) {
    this.originColumns = originColumns;
  }
}
