package com.hufudb.onedb.owner.adapter;

public interface AdapterFactory {
  Adapter create(AdapterConfig config);
  String getType();
}
