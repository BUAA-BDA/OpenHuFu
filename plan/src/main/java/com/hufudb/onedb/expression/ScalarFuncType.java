package com.hufudb.onedb.expression;

import com.google.common.collect.ImmutableMap;

public enum ScalarFuncType {
  ABS("ABS", 0);

  private final static ImmutableMap<Integer, ScalarFuncType> MAP;

  private final String name;
  private final int id;

  static {
    final ImmutableMap.Builder<Integer, ScalarFuncType> builder = ImmutableMap.builder();
    for (ScalarFuncType func : values()) {
      builder.put(func.getId(), func);
    }
    MAP = builder.build();
  }

  ScalarFuncType(String name, int id) {
    this.name = name;
    this.id = id;
  }

  String getName() {
    return name;
  }

  int getId() {
    return id;
  }

  public static ScalarFuncType of(int id) {
    return MAP.get(id);
  }
}
