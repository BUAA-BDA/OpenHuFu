package com.hufudb.onedb.udf;

import java.util.List;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class Point implements ScalarUDF {

  @Override
  public String getName() {
    return "point";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.POINT;
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 2) {
      LOG.error("Point UDF expect 2 parameters, but give {}", inputs.size());
      throw new RuntimeException("Point UDF expect 2 parameters");
    }
    if (inputs.get(0) == null || inputs.get(1) == null) {
      return null;
    }
    return new com.hufudb.onedb.data.storage.Point(((Number) inputs.get(0)).doubleValue(), ((Number) inputs.get(1)).doubleValue());
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    switch (dataSource) {
      case "postgis":
        return String.format("'SRID=4326;POINT(%s %s)'", inputs.get(0), inputs.get(1));
      default:
        throw new RuntimeException("Unsupported datasource for Point UDF");
    }
  }
}
