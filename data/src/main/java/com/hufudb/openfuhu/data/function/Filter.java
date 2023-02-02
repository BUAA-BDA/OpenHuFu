package com.hufudb.openhufu.data.function;

import com.hufudb.openhufu.data.storage.Row;
/**
 * filter function, reseved when return true
 */
public interface Filter {
  boolean filter(Row row);
}
