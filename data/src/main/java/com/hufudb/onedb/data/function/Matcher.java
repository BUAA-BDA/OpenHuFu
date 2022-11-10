package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.Row;

public interface Matcher {
  boolean match(Row r1, Row r2);
}
