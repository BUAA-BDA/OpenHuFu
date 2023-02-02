package com.hufudb.openhufu.expression;

import com.google.common.collect.ImmutableMap;

public enum ScalarFuncType {
  ABS("abs", 1, Number.class);

  private final static ImmutableMap<String, ScalarFuncType> MAP;

  private final String name;
  private final int id;
  private final Object[] argumentTypes;

  static {
    final ImmutableMap.Builder<String, ScalarFuncType> builder = ImmutableMap.builder();
    for (ScalarFuncType func : values()) {
      builder.put(func.getName(), func);
    }
    MAP = builder.build();
  }

  ScalarFuncType(String name, int id, Class... argumentTypes) {
    this.name = name;
    this.id = id;
    this.argumentTypes = argumentTypes;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public static ScalarFuncType of(String name) {
    return MAP.get(name);
  }

  public static boolean support(String name) {
    return MAP.containsKey(name);
  }
}
