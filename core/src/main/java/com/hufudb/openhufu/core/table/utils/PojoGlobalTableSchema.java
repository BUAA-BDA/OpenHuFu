package com.hufudb.openhufu.core.table.utils;

import com.hufudb.openhufu.core.table.LocalTableConfig;
import com.hufudb.openhufu.core.table.FQTableSchema;
import com.hufudb.openhufu.data.schema.utils.PojoSchema;
import java.util.ArrayList;
import java.util.List;

public class PojoGlobalTableSchema {
  public String name;
  public PojoSchema schema;
  public List<LocalTableConfig> mappings;

  public PojoGlobalTableSchema() {}

  public static PojoGlobalTableSchema from(FQTableSchema info) {
    PojoGlobalTableSchema sinfo = new PojoGlobalTableSchema();
    sinfo.setName(info.getName());
    sinfo.setSchema(PojoSchema.fromSchema(info.getSchema()));
    sinfo.setMappings(info.getMappings());
    return sinfo;
  }

  public static List<PojoGlobalTableSchema> from(List<FQTableSchema> info) {
    List<PojoGlobalTableSchema> sinfo = new ArrayList<>();
    for (FQTableSchema i : info) {
      sinfo.add(PojoGlobalTableSchema.from(i));
    }
    return sinfo;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public PojoSchema getSchema() {
    return schema;
  }

  public void setSchema(PojoSchema schema) {
    this.schema = schema;
  }

  public List<LocalTableConfig> getMappings() {
    return mappings;
  }

  public void setMappings(List<LocalTableConfig> mappings) {
    this.mappings = mappings;
  }
}
