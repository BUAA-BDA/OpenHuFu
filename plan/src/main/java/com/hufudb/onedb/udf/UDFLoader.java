package com.hufudb.onedb.udf;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;

public class UDFLoader {
  private static final Logger LOG = LoggerFactory.getLogger(UDFLoader.class);

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
    ClassLoader udfClassLoader = new URLClassLoader(udfURLs.toArray(new URL[0]), ScalarUDF.class.getClassLoader());
    ServiceLoader<ScalarUDF> udfServices = ServiceLoader.load(ScalarUDF.class, udfClassLoader);
    ImmutableMap.Builder<String, ScalarUDF> scalarUDFs = ImmutableMap.builder();
    for (ScalarUDF scalar : udfServices) {
      scalarUDFs.put(scalar.getName(), scalar);
      LOG.info("get scalar udf {}", scalar.getClass().getName());
    }
    return scalarUDFs.build();
  }
}
