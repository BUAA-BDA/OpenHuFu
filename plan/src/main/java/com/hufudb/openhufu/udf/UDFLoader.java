package com.hufudb.openhufu.udf;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;

public class UDFLoader {
  private static final Logger LOG = LoggerFactory.getLogger(UDFLoader.class);
  public static Map<String, ScalarUDF> scalarUDFs;

  static {
    scalarUDFs = UDFLoader.loadScalarUDF();
  }

  private UDFLoader() {}

  public static Map<String, ScalarUDF> loadScalarUDF() {
    ServiceLoader<ScalarUDF> udfServices = ServiceLoader.load(ScalarUDF.class, ScalarUDF.class.getClassLoader());
    ImmutableMap.Builder<String, ScalarUDF> scalarUDFs = ImmutableMap.builder();
    for (ScalarUDF scalar : udfServices) {
      scalarUDFs.put(scalar.getName(), scalar);
      LOG.info("load scalar udf {} success", scalar.getClass().getName());
    }
    return scalarUDFs.build();
  }

  public static Object implementScalar(String funcName, List<Object> inputs) {
    if (!UDFLoader.scalarUDFs.containsKey(funcName)) {
      LOG.error("Unsupported scalar UDF {}", funcName);
      throw new RuntimeException("Unsupported scalar UDF");
    }
    return UDFLoader.scalarUDFs.get(funcName).implement(inputs);
  }

  public static ColumnType getScalarOutType(String funcName, List<Expression> inputs) {
    return UDFLoader.scalarUDFs.get(funcName)
        .getOutType(inputs.stream().map(in -> in.getOutType()).collect(Collectors.toList()));
  }
}
