package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.Row;

public interface Aggregator {
  void set(DataSetIterator iterator);
  Row aggregate();
  boolean hasNext();
}
