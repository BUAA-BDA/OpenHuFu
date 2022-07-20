package com.hufudb.onedb.data.function;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 对多个T对象进行聚合
 * 返回一个E对象
 * 
 * aggregate on multiple T,
 * return E
 */
public interface AggregateFunction<T, E> {
  /**
   * 向聚合集合添加一个元素T
   * 
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
