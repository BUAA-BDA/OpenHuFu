package com.hufudb.openhufu.owner.adapter;

import com.hufudb.openhufu.common.enums.DataSourceType;

/**
 * Adapter configurations
 * add new config item to support more adapters
 */
public class AdapterConfig {
  public DataSourceType datasource;
  // for jdbc adapter
  public String catalog;
  public String url;
  public String delimiter;
  public String user;
  public String passwd;

  public AdapterConfig() {}

  public AdapterConfig(DataSourceType datasource) {
    this.datasource = datasource;
  }
}
