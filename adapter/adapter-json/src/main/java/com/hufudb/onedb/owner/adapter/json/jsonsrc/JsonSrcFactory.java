package com.hufudb.onedb.owner.adapter.json.jsonsrc;

import com.hufudb.onedb.owner.adapter.AdapterConfig;

import java.util.List;

public interface JsonSrcFactory {
  public JsonSrc createJsonSrc(String tableName);
  public List<String> getTableNames(AdapterConfig config);
  public String getType();
}
