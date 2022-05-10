package com.hufudb.onedb.owner.adapter;

import com.hufudb.onedb.core.data.FieldType;

public interface AdapterTypeConverter {
  FieldType convert(String typeName);
}
