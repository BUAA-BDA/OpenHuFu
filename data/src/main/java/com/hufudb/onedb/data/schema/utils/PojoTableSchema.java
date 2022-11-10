package com.hufudb.onedb.data.schema.utils;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.TableSchema;

public class PojoTableSchema {
  public String name;
  public PojoSchema schema;

  public static PojoTableSchema from(TableSchema schema) {
    PojoTableSchema pschema = new PojoTableSchema();
    pschema.name = schema.getName();
    pschema.schema = PojoSchema.fromSchema(schema.getSchema());
    return pschema;
  }

  public static List<PojoTableSchema> from(List<TableSchema> schemas) {
    return schemas.stream().map(sc -> from(sc)).collect(Collectors.toList());
  }
}
