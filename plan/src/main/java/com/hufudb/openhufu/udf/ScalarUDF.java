package com.hufudb.openhufu.udf;

import java.util.List;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ScalarUDF {
  static final Logger LOG = LoggerFactory.getLogger(ScalarUDF.class);

  String getName();
  ColumnType getOutType(List<ColumnType> inTypes);
  Object implement(List<Object> inputs);
  String translate(String dataSource, List<String> inputs);
}
