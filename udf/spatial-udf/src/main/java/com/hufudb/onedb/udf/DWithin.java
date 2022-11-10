package com.hufudb.onedb.udf;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.storage.Point;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class DWithin implements ScalarUDF {

  @Override
  public String getName() {
    return "dwithin";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.BOOLEAN;
  }

  public Boolean dwithin(Point left, Point right, Double distance) {
    return (Boolean) implement(ImmutableList.of(left, right, distance));
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 3) {
      LOG.error("DWithin UDF expect 3 parameters, but give {}", inputs.size());
      throw new RuntimeException("DWithin UDF expect 3 parameters");
    }
    if (inputs.get(0) == null || inputs.get(1) == null || inputs.get(2) == null) {
      return null;
    }
    if (!(inputs.get(0) instanceof Point) || !(inputs.get(1) instanceof Point)
        || !(inputs.get(2) instanceof Number)) {
      LOG.error("DWithin UDF requires (Point, Point, Double)");
      throw new RuntimeException("DWithin UDF requires (Point, Point)");
    }
    Point left = (Point) inputs.get(0);
    Point right = (Point) inputs.get(1);
    return Math.sqrt((left.getX() - right.getX()) * (left.getX() - right.getX())
        + (left.getY() - right.getY()) * (left.getY() - right.getY())) <= ((Number) inputs.get(2))
            .doubleValue();
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    switch(dataSource) {
      case "postgis":
        return String.format("ST_DWithin(%s, %s, %s)", inputs.get(0), inputs.get(1), inputs.get(2));
      default:
        throw new RuntimeException("Unsupported datasource for Distance UDF");
    }
  }
}
