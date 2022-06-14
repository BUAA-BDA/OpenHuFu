package com.hufudb.onedb.owner.adapter.csv;

import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;

public class CsvAdapterFactory implements AdapterFactory {

  public CsvAdapterFactory() {
  }

  @Override
  public Adapter create(AdapterConfig config) {
    assert(config.datasource.equals("csv"));
    try {
      return new CsvAdapter(config.url);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getType() {
    return "csv";
  }
}
