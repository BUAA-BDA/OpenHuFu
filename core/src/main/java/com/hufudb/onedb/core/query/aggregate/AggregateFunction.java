package com.hufudb.onedb.core.query.aggregate;

import java.util.List;
import java.util.stream.Collectors;

public interface AggregateFunction<T, E> {
  void add(T ele);
  E aggregate();
  AggregateFunction<T, E> patternCopy();

  public static <T, E> List<AggregateFunction<T, E>> patternCopy(List<AggregateFunction<T, E>> funcs) {
    return funcs.stream().map(func -> func.patternCopy()).collect(Collectors.toList());
  }
}
