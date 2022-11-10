package com.hufudb.onedb.data.schema.utils;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.PublishedTableSchema;

/**
 * Read from .json file by Gson
 */
public class PojoPublishedTableSchema {
  public String publishedName;
  public String actualName;
  public List<PojoColumnDesc> publishedColumns;

  public PojoPublishedTableSchema() {}

  public PojoPublishedTableSchema(
        String publishedName,
        String actualName,
        List<PojoColumnDesc> publishedColumns) {
    this.publishedName = publishedName;
    this.actualName = actualName;
    this.publishedColumns = publishedColumns;
  }

  public static PojoPublishedTableSchema from(PublishedTableSchema schema) {
    return new PojoPublishedTableSchema(
        schema.getPublishedTableName(),
        schema.getActualTableName(),
        PojoColumnDesc.fromColumnDesc(schema.getFakeTableSchema().getSchema().getColumnDescs()));
  }

  public static List<PojoPublishedTableSchema> from(List<PublishedTableSchema> tableSchemas) {
    return tableSchemas.stream().map(info -> from(info)).collect(Collectors.toList());
  }

  public String getPublishedName() {
    return publishedName;
  }

  public void setPublishedName(String publishedTableName) {
    this.publishedName = publishedTableName;
  }

  public String getActualName() {
    return actualName;
  }

  public void setActualName(String originTableName) {
    this.actualName = originTableName;
  }

  public List<PojoColumnDesc> getPublishedColumns() {
    return publishedColumns;
  }

  public void setPublishedColumns(List<PojoColumnDesc> publishedColumns) {
    this.publishedColumns = publishedColumns;
  }

}
