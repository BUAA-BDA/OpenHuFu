package com.hufudb.openhufu.owner.adapter.csv;

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
    assert(config.datasource.equals("csv"));
    try {
      return new CsvAdapter(config.url);
    } catch (Exception e) {
      LOG.error("Fail to create csv adapter: {}", config.url, e);
      return null;
    }
  }

  @Override
  public String getType() {
    return "csv";
  }
}
