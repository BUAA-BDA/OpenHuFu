package com.hufudb.onedb.data.schema.utils;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.Schema;

public class PojoSchema {
  public List<PojoColumnDesc> columns;

  public static PojoSchema fromSchema(Schema schema) {
    PojoSchema pschema = new PojoSchema();
    pschema.columns = schema.getColumnDescs().stream().map(
      col -> PojoColumnDesc.fromColumnDesc(col)
    ).collect(Collectors.toList());
    return pschema;
  }
}
