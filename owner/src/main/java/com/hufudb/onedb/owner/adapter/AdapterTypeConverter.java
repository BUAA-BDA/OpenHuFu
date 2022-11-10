package com.hufudb.onedb.owner.adapter;

import com.hufudb.onedb.proto.OneDBData.ColumnType;

public interface AdapterTypeConverter {
  ColumnType convert(String typeName);
}
