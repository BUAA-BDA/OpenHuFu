package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.Row;

public interface Mapping {
  Object map(Row row);
}
