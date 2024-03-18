package com.hufudb.openhufu.udf;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.List;

public class Vector implements ScalarUDF {
  public final static GeometryFactory geoFactory = new GeometryFactory();

  @Override
  public String getName() {
    return "vector";
  }

  @Override
  public ColumnType getOutType(List<ColumnType> inTypes) {
    return ColumnType.VECTOR;
  }

  public String vector(String input) {
    return (String) implement(ImmutableList.of(input));
  }

  @Override
  public Object implement(List<Object> inputs) {
    if (inputs.size() != 1) {
      LOG.error("Vector UDF expect 1 parameters, but give {}", inputs.size());
      throw new RuntimeException("Vector UDF expect 1 parameters");
    }

    return inputs.get(0);
  }

  @Override
  public String translate(String dataSource, List<String> inputs) {
    switch (dataSource) {
      case "postgis":
        return String.format("'%s'", inputs.get(0));
      default:
        throw new RuntimeException("Unsupported datasource for Vector UDF");
    }
  }
}
