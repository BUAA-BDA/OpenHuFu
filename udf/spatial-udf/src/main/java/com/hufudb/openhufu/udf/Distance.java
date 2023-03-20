package com.hufudb.openhufu.udf;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.storage.Point;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;

public class Distance implements ScalarUDF {

  @Override
  public String getName() {
    return "distance";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.DOUBLE;
  }
  public Double distance(Point left, Point right) {
    return (Double) implement(ImmutableList.of(left, right));
  }
  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 2) {
      LOG.error("Distance UDF expect 2 parameters, but give {}", inputs.size());
      throw new RuntimeException("Distance UDF expect 2 parameters");
    }
    if (inputs.get(0) == null || inputs.get(1) == null) {
      return null;
    }
    if (!(inputs.get(0) instanceof Point) || !(inputs.get(1) instanceof Point)) {
      LOG.error("Distance UDF requires (Point, Point)");
      throw new RuntimeException("Distance UDF requires (Point, Point)");
    }
    Point left = (Point) inputs.get(0);
    Point right = (Point) inputs.get(1);
    return Math.sqrt((left.getX() - right.getX()) * (left.getX() - right.getX())
        + (left.getY() - right.getY()) * (left.getY() - right.getY()));
  }
}
