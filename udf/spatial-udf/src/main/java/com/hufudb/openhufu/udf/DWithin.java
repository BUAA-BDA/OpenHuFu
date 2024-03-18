package com.hufudb.openhufu.udf;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.locationtech.jts.geom.Geometry;

public class DWithin implements ScalarUDF {

  @Override
  public String getName() {
    return "dwithin";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.BOOLEAN;
  }

  public Boolean dwithin(Object left, Object right, Double distance) {
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
    if (!(inputs.get(0) instanceof Geometry) || !(inputs.get(1) instanceof Geometry)
        || !(inputs.get(2) instanceof Number)) {
      LOG.error("DWithin UDF requires (Point, Point, Double)");
      throw new RuntimeException("DWithin UDF requires (Point, Point)");
    }
    Geometry left = (Geometry) inputs.get(0);
    Geometry right = (Geometry) inputs.get(1);
    return left.distance(right) <= ((Number) inputs.get(2))
            .doubleValue();
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    switch(dataSource) {
      case "postgis":
        if (inputs.get(0).toLowerCase().contains("point (") || inputs.get(1).toLowerCase().contains("point (")) {
          return String.format("ST_DWithin(%s, %s, %s)", inputs.get(0), inputs.get(1), inputs.get(2));
        }
        else {
          return String.format("l2_distance(%s, %s) < %s", inputs.get(0), inputs.get(1), inputs.get(2));
        }
      default:
        throw new RuntimeException("Unsupported datasource for Distance UDF");
    }
  }
}
