package com.hufudb.openhufu.udf;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class KNN implements ScalarUDF {

  @Override
  public String getName() {
    return "knn";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.BOOLEAN;
  }

  public Boolean knn(Geometry left, Geometry right, Integer count) {
    return (Boolean) implement(ImmutableList.of(left, right, count));
  }

  @Override
  public Object implement(List<Object> inputs) {
    throw new RuntimeException();
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    return "";
  }
}
