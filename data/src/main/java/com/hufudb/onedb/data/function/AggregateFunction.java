package com.hufudb.onedb.data.function;

import java.util.List;
import java.util.stream.Collectors;

public interface AggregateFunction<T, E> {
  void add(T ele);
  E aggregate();
  AggregateFunction<T, E> copy();

  public static <T, E> List<AggregateFunction<T, E>> copy(List<AggregateFunction<T, E>> funcs) {
    return funcs.stream().map(func -> func.copy()).collect(Collectors.toList());
  }
}
