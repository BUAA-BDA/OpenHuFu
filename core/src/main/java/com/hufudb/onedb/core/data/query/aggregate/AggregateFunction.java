package com.hufudb.onedb.core.data.query.aggregate;

public interface AggregateFunction<T, E> {
  void add(T ele);
  E aggregate();
}
