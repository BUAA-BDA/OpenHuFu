package com.hufudb.onedb.data.function;

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
  void add(T ele);
  E aggregate();
  AggregateFunction<T, E> copy();

  public static <T, E> List<AggregateFunction<T, E>> copy(List<AggregateFunction<T, E>> funcs) {
    return funcs.stream().map(func -> func.copy()).collect(Collectors.toList());
  }
}
