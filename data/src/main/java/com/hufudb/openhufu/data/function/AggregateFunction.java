package com.hufudb.openhufu.data.function;

import java.util.List;
import java.util.stream.Collectors;

/**
 * aggregate on multiple T,
 * return E
 */
public interface AggregateFunction<T, E> {
  /**
   * add a T element to the set which is to be aggregated,
   * meanwhile, update the output value (which is a E)
   */
  // local add
  void add(T ele);
  // global aggregate
  E aggregate();
  AggregateFunction<T, E> copy();

  static <T, E> List<AggregateFunction<T, E>> copy(List<AggregateFunction<T, E>> funcs) {
    return funcs.stream().map(func -> func.copy()).collect(Collectors.toList());
  }
}
