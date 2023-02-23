package com.hufudb.openhufu.owner.adapter;

import com.hufudb.openhufu.common.enums.DataSourceType;
import com.hufudb.openhufu.data.schema.utils.PojoTableSchema;
import java.util.List;

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

  public List<PojoTableSchema> tables;

  public AdapterConfig() {}

  public AdapterConfig(DataSourceType datasource) {
    this.datasource = datasource;
  }
}
