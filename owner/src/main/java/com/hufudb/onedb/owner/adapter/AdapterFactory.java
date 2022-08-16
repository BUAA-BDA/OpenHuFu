package com.hufudb.onedb.owner.adapter;

import java.util.Map;

public interface AdapterFactory {
  Adapter create(AdapterConfig config);
  String getType();

  public static Adapter loadAdapter(AdapterConfig config, String adapterDir) {
    Map<String, AdapterFactory> adapterFactories =
        AdapterLoader.loadAdapters(adapterDir);
    AdapterFactory factory = adapterFactories.get(config.datasource);
    if (factory == null) {
      throw new RuntimeException("Fail to get adapter for datasource");
    }
    return factory.create(config);
  }
}
