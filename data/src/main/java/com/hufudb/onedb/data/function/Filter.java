package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.Row;
/**
 * filter function, reseved when return true
 */
public interface Filter {
  boolean filter(Row row);
}
