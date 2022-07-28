package com.hufudb.onedb.udf;

import java.util.List;
import com.hufudb.onedb.data.storage.Point;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class Distance implements ScalarUDF {

  @Override
  public String getName() {
    return "Distance";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.DOUBLE;
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 2) {
      LOG.error("Distance UDF expect 2 parameters, but give {}", inputs.size());
      throw new RuntimeException("Distance UDF expect 2 parameters");
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

  @Override
  public String toString(String dataSource, List<String> inputs) {
    switch(dataSource) {
      case "postgis":
        return String.format("%s <-> %s", inputs.get(0), inputs.get(1));
      default:
        throw new RuntimeException("Unsupported datasource for Distance UDF");
    }
  }
}
