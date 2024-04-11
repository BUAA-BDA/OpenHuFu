package com.hufudb.openhufu.udf;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
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

  public Boolean dwithin(Geometry left, Geometry right, Double distance) {
    return (Boolean) implement(ImmutableList.of(left, right, distance));
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 3) {
      LOG.error("DWithin UDF expect 3 parameters, but give {}", inputs.size());
      throw new OpenHuFuException(ErrorCode.FUNCTION_PARAMS_SIZE_ERROR, getName(), 3,
          inputs.size());
    }
    if (inputs.get(0) == null || inputs.get(1) == null || inputs.get(2) == null) {
      return null;
    }
    if (!(inputs.get(0) instanceof Geometry) || !(inputs.get(1) instanceof Geometry)
        || !(inputs.get(2) instanceof Number)) {
      LOG.error("DWithin UDF requires (Point, Point, Double)");
      throw new OpenHuFuException(ErrorCode.FUNCTION_PARAMS_TYPE_ERROR, getName(),
          "(Point, Point, Double)");
    }
    Geometry left = (Geometry) inputs.get(0);
    Geometry right = (Geometry) inputs.get(1);
    return left.distance(right) <= ((Number) inputs.get(2))
        .doubleValue();
  }
}
