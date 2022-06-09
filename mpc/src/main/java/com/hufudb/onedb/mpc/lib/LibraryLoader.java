package com.hufudb.onedb.mpc.lib;
import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.mpc.ProtocolFactory;
import com.hufudb.onedb.mpc.ProtocolType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LibraryLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LibraryLoader.class);

  public static Map<ProtocolType, ProtocolFactory> loadProtocolLibrary(String libDir) {
    LOG.info("Load library from {}", libDir);
    File libJars[]= new File(libDir).listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.getName().endsWith(".jar");
        }
      });
    List<URL> libURLs = new ArrayList<>();
    for (File libJar : libJars) {
      try {
        libURLs.add(libJar.toURI().toURL());
        LOG.info("Add JAR {}", libJar.getAbsolutePath());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    ClassLoader libClassLoader = new URLClassLoader(libURLs.toArray(new URL[0]), ProtocolFactory.class.getClassLoader());
    ServiceLoader<ProtocolFactory> libs = ServiceLoader.load(ProtocolFactory.class, libClassLoader);
    ImmutableMap.Builder<ProtocolType, ProtocolFactory> builder = ImmutableMap.builder();
    for (ProtocolFactory lib : libs) {
      LOG.info("Load library of protocol {}", lib.getType());
      builder.put(lib.getType(), lib);
    }
    try {
      return builder.build();
    } catch (IllegalArgumentException e) {
      LOG.error("Duplicate protocol type found: {}", e.getMessage());
      return ImmutableMap.of();
    }
  }
}
