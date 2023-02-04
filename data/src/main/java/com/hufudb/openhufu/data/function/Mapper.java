package com.hufudb.openhufu.data.function;

import com.hufudb.openhufu.data.storage.Row;

public interface Mapper {
  Object map(Row row);
}
