package com.hufudb.openhufu.udf;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class Point implements ScalarUDF {
  public final static GeometryFactory geoFactory = new GeometryFactory();

  @Override
  public String getName() {
    return "point";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.GEOMETRY;
  }

  public Geometry point(Double x, Double y) {
    return (Geometry) implement(ImmutableList.of(x, y));
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 2) {
      LOG.error("Point UDF expect 2 parameters, but give {}", inputs.size());
      throw new OpenHuFuException(ErrorCode.FUNCTION_PARAMS_SIZE_ERROR, getName(), 2,
          inputs.size());
    }
    if (inputs.get(0) == null || inputs.get(1) == null) {
      return null;
    }
    Coordinate coordinate = new Coordinate(((Number) inputs.get(0)).doubleValue(),
        ((Number) inputs.get(1)).doubleValue());
    return geoFactory.createPoint(coordinate);
  }
}
