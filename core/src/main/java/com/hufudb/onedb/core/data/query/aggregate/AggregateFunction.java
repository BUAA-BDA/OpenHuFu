package com.hufudb.onedb.core.data.query.aggregate;

public interface AggregateFunction<T> {
  void add(T ele);
  T aggregate();
}
