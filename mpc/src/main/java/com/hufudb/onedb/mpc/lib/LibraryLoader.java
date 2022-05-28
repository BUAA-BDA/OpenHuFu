package com.hufudb.onedb.mpc.lib;
import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import com.hufudb.onedb.mpc.ProtocolFactory;
import com.hufudb.onedb.mpc.ProtocolType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LibraryLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LibraryLoader.class);

  public static ProtocolFactory loadProtocolLibrary(String libDir, ProtocolType type) {
    LOG.info("Load library {} from {}", type, libDir);
    File libJars[]= new File(libDir).listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.getName().endsWith(".jar");
        }
      });
    List<URL> libURLs = new ArrayList<>(libJars.length);
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
    for (ProtocolFactory lib : libs) {
      if (lib.getType().equals(type)) {
        LOG.info("Successfully load library of protocol {}", type);
        return lib;
      }
    }
    LOG.warn("Fail to load library of protocol {}", type);
    return null;
  }
}
