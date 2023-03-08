package com.hufudb.openhufu.udf;

import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import java.util.List;
import com.google.common.collect.ImmutableList;

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
    return new com.hufudb.openhufu.data.storage.Point(((Number) inputs.get(0)).doubleValue(), ((Number) inputs.get(1)).doubleValue());
  }
}
