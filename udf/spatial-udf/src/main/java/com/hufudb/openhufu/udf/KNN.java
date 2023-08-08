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

  public Boolean knn(Geometry left, Geometry right, Integer distance) {
    return (Boolean) implement(ImmutableList.of(left, right, distance));
  }

  @Override
  public Object implement(List<Object> inputs) {
    throw new RuntimeException();
//    if (inputs.size() != 3) {
//      LOG.error("KNN UDF expect 3 parameters, but give {}", inputs.size());
//      throw new RuntimeException("KNN UDF expect 3 parameters");
//    }
//    if (inputs.get(0) == null || inputs.get(1) == null || inputs.get(2) == null) {
//      return null;
//    }
//    if (!(inputs.get(0) instanceof Geometry) || !(inputs.get(1) instanceof Geometry)
//        || !(inputs.get(2) instanceof Integer)) {
//      LOG.error("KNN UDF requires (Point, Point, Integer)");
//      throw new RuntimeException("KNN UDF requires (Point, Point)");
//    }
//    Geometry left = (Geometry) inputs.get(0);
//    Geometry right = (Geometry) inputs.get(1);
//    return true;
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    throw new RuntimeException();
  }
}
