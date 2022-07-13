package com.hufudb.onedb.owner.adapter.json;

import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.json.jsonsrc.JsonSrcFactory;
import com.hufudb.onedb.owner.adapter.json.jsonsrc.JsonTextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class JsonAdapterFactory implements AdapterFactory {

  private final static Logger LOG = LoggerFactory.getLogger(JsonAdapterFactory.class);
  private final Map<String, JsonSrcFactory> jsonSrcFactoryMap;

  public JsonAdapterFactory() {
    String oneDBRoot = System.getenv("ONEDB_ROOT");
    Path jsonSrcDir = Paths.get(oneDBRoot, "adapter", "jsonSrc");
    jsonSrcFactoryMap = loadAdapters(jsonSrcDir.toString());
  }

  private Map<String, JsonSrcFactory> loadAdapters(String jsonSrcDirectory) {
    LOG.info("Load jsonSrc from {}", jsonSrcDirectory);
    File[] jsonSrcs = new File(jsonSrcDirectory).listFiles(file -> file.getName().endsWith(".jar"));
    assert jsonSrcs != null;
    List<URL> jsonSrcURLs = new ArrayList<>(jsonSrcs.length);
    for (File jsonSrc : jsonSrcs) {
      try {
        jsonSrcURLs.add(jsonSrc.toURI().toURL());
        LOG.info("Add JAR {}", jsonSrc.getAbsolutePath());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    ClassLoader jsonSrcClassLoader = new URLClassLoader(jsonSrcURLs.toArray(new URL[0]), JsonSrcFactory.class.getClassLoader());
    ServiceLoader<JsonSrcFactory> jsonSrcServices = ServiceLoader.load(JsonSrcFactory.class, jsonSrcClassLoader);
    ImmutableMap.Builder<String, JsonSrcFactory> jsonSrcFactories = ImmutableMap.builder();
    for (JsonSrcFactory factory : jsonSrcServices) {
      jsonSrcFactories.put(factory.getType(), factory);
      LOG.info("get jsonSrc factory {}", factory.getClass().getName());
    }
    return jsonSrcFactories.build();
  }

  @Override
  public Adapter create(AdapterConfig config) {
    if (jsonSrcFactoryMap.containsKey(config.catalog)) {
      return new JsonAdapter(jsonSrcFactoryMap.get(config.catalog), config);
    } else if (config.catalog.equals("text")) {
      return new JsonAdapter(new JsonTextFactory(), config);
    } else {
      throw new RuntimeException("not support adapter type");
    }
  }

  @Override
  public String getType() {
    return "json";
  }
}
