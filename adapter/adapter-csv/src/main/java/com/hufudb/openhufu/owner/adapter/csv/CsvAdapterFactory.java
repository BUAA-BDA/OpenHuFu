package com.hufudb.openhufu.owner.adapter.csv;

import com.hufudb.openhufu.common.enums.DataSourceType;
import com.hufudb.openhufu.owner.adapter.AdapterFactory;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.adapter.AdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvAdapterFactory implements AdapterFactory {

  protected final static Logger LOG = LoggerFactory.getLogger(CsvAdapterFactory.class);

  public CsvAdapterFactory() {
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource == DataSourceType.CSV);
    try {
      return new CsvAdapter(config);
    } catch (Exception e) {
      LOG.error("Fail to create csv adapter: {}", config.url, e);
      return null;
    }
  }

  @Override
  public DataSourceType getType() {
    return DataSourceType.CSV;
  }
}
