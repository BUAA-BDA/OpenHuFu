package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.Row;

public interface Mapper {
  Object map(Row row);
}
