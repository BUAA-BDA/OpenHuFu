package com.hufudb.onedb.data.schema.utils;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.PublishedTableSchema;

public class PojoPublishedTableSchema {
  public String publishedName;
  public String actualName;
  public List<PojoColumnDesc> publishedColumns;
  public List<Integer> actualColumns;

  public PojoPublishedTableSchema(
        String publishedTableName,
        String actualTableName,
        List<PojoColumnDesc> publishedFields,
        List<Integer> actualColumns) {
    this.publishedName = publishedTableName;
    this.actualName = actualTableName;
    this.publishedColumns = publishedFields;
    this.actualColumns = actualColumns;
  }

  public static PojoPublishedTableSchema from(PublishedTableSchema schema) {
    return new PojoPublishedTableSchema(
        schema.getPublishedTableName(),
        schema.getActualTableName(),
        PojoColumnDesc.fromColumnDesc(schema.getFakeTableSchema().getSchema().getColumnDescs()),
        schema.getMappings());
  }

  public static List<PojoPublishedTableSchema> from(List<PublishedTableSchema> tableSchemas) {
    return tableSchemas.stream().map(info -> from(info)).collect(Collectors.toList());
  }

  public String getPublishedTableName() {
    return publishedName;
  }

  public void setPublishedTableName(String publishedTableName) {
    this.publishedName = publishedTableName;
  }

  public String getActualTableName() {
    return actualName;
  }

  public void setActualTableName(String originTableName) {
    this.actualName = originTableName;
  }

  public List<PojoColumnDesc> getPublishedColumns() {
    return publishedColumns;
  }

  public void setPublishedColumns(List<PojoColumnDesc> publishedColumns) {
    this.publishedColumns = publishedColumns;
  }

  public List<Integer> getActualColumns() {
    return actualColumns;
  }

  public void setActualColumns(List<Integer> actualColumns) {
    this.actualColumns = actualColumns;
  }
}
