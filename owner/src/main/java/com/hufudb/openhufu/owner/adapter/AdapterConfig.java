package com.hufudb.openhufu.owner.adapter;

/**
 * Adapter configurations
 * add new config item to support more adapters
 */
public class AdapterConfig {
  public String datasource;
  // for jdbc adapter
  public String catalog;
  public String url;
  public String user;
  public String passwd;

  public AdapterConfig() {}

  public AdapterConfig(String datasource) {
    this.datasource = datasource;
  }
}
