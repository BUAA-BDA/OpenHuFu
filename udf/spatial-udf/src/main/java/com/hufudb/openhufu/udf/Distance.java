package com.hufudb.openhufu.udf;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.locationtech.jts.geom.Geometry;

public class Distance implements ScalarUDF {

  @Override
  public String getName() {
    return "distance";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.DOUBLE;
  }

  public Double distance(Geometry left, Geometry right) {

    return (Double) implement(ImmutableList.of(left, right));
  }
  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 2) {
      LOG.error("Distance UDF expect 2 parameters, but give {}", inputs.size());
      throw new OpenHuFuException(ErrorCode.FUNCTION_PARAMS_SIZE_ERROR, getName(), 2,
          inputs.size());
    }
    if (inputs.get(0) == null || inputs.get(1) == null) {
      return null;
    }
    if (!(inputs.get(0) instanceof Geometry) || !(inputs.get(1) instanceof Geometry)) {
      LOG.error("Distance UDF requires (Point, Point)");
      throw new RuntimeException("Distance UDF requires (Point, Point)");
    }
    Geometry left = (Geometry) inputs.get(0);
    Geometry right = (Geometry) inputs.get(1);
    return left.distance(right);
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    switch(dataSource) {
      case "POSTGIS":
        return String.format("%s <-> %s", inputs.get(0), inputs.get(1));
      default:
        throw new RuntimeException("Unsupported datasource for Distance UDF");
    }
  }
}
