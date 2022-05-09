package com.hufudb.onedb.owner.adapter;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterLoader {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterLoader.class);

  void loadAdapter(String adapterDirectory) {
    File adapters[]= new File(adapterDirectory).listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().endsWith(".jar");
      }
    });
    List<URL> adapterURLs = new ArrayList<>(adapters.length);
    for (File adapter : adapters) {
      try {
        adapterURLs.add(adapter.toURI().toURL());
        LOG.info("Add JAR {}", adapter.getAbsolutePath());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    ClassLoader adapterClassLoader = new URLClassLoader(adapterURLs.toArray(new URL[0]), DataSourceAdapter.class.getClassLoader());
    ServiceLoader<DataSourceAdapter> adapterServices = ServiceLoader.load(DataSourceAdapter.class, adapterClassLoader);
    for (DataSourceAdapter adapter : adapterServices) {
      LOG.info("get adapter {}", adapter.getClass().getName());
    }
  }
}
