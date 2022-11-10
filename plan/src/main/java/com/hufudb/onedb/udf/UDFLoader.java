package com.hufudb.onedb.udf;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;

public class UDFLoader {
  private static final Logger LOG = LoggerFactory.getLogger(UDFLoader.class);
  public static Map<String, ScalarUDF> scalarUDFs;

  static {
    Path libDir = Paths.get(System.getenv("ONEDB_ROOT"), "udf", "scalar");
    scalarUDFs = UDFLoader.loadScalarUDF(libDir.toString());
  }

  private UDFLoader() {}

  public static Map<String, ScalarUDF> loadScalarUDF(String scalarUDFDirectory) {
    LOG.info("Load scalar udf from {}", scalarUDFDirectory);
    File udfs[] = new File(scalarUDFDirectory).listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().endsWith(".jar");
      }
    });
    List<URL> udfURLs = new ArrayList<>(udfs.length);
    for (File udf : udfs) {
      try {
        udfURLs.add(udf.toURI().toURL());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    ClassLoader udfClassLoader =
        new URLClassLoader(udfURLs.toArray(new URL[0]), ScalarUDF.class.getClassLoader());
    ServiceLoader<ScalarUDF> udfServices = ServiceLoader.load(ScalarUDF.class, udfClassLoader);
    ImmutableMap.Builder<String, ScalarUDF> scalarUDFs = ImmutableMap.builder();
    for (ScalarUDF scalar : udfServices) {
      scalarUDFs.put(scalar.getName(), scalar);
      LOG.info("get scalar udf {}", scalar.getClass().getName());
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

  public static String translateScalar(String funcName, String dataSource, List<String> inputs) {
    if (!UDFLoader.scalarUDFs.containsKey(funcName)) {
      LOG.error("Unsupported scalar UDF {}", funcName);
      throw new RuntimeException("Unsupported scalar UDF");
    }
    return UDFLoader.scalarUDFs.get(funcName).translate(dataSource, inputs);
  }

  public static ColumnType getScalarOutType(String funcName, List<Expression> inputs) {
    return UDFLoader.scalarUDFs.get(funcName)
        .getOutType(inputs.stream().map(in -> in.getOutType()).collect(Collectors.toList()));
  }
}
