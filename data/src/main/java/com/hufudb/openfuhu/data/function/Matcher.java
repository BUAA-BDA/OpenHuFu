package com.hufudb.openhufu.data.function;

import com.hufudb.openhufu.data.storage.Row;

public interface Matcher {
  boolean match(Row r1, Row r2);
}
