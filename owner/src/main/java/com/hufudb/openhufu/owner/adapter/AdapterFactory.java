package com.hufudb.openhufu.owner.adapter;

import com.hufudb.openhufu.common.enums.DataSourceType;
import java.util.Map;

public interface AdapterFactory {
  Adapter create(AdapterConfig config);
  DataSourceType getType();

  public static Adapter loadAdapter(AdapterConfig config, String adapterDir) {
    Map<DataSourceType, AdapterFactory> adapterFactories =
        AdapterLoader.loadAdapters(adapterDir);
    AdapterFactory factory = adapterFactories.get(config.datasource);
    if (factory == null) {
      throw new RuntimeException("Fail to get adapter for datasource");
    }
    return factory.create(config);
  }
}
