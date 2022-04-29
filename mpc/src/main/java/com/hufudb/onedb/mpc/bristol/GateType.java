package com.hufudb.onedb.mpc.bristol;

import com.google.common.collect.ImmutableMap;

public enum GateType {
  AND("AND", 0),
  XOR("XOR", 1),
  NOT("INV", 2),
  UNSUPPORT("UNSUPPORT", 3);

  private static final ImmutableMap<Integer, GateType> MAP;

  static {
    final ImmutableMap.Builder<Integer, GateType> builder = ImmutableMap.builder();
    for (GateType type : values()) {
      builder.put(type.id, type);
    }
    MAP = builder.build();
  }

  private final String name;
  private final int id;

  GateType(String name, int id) {
    this.name = name;
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return name;
  }

  public static GateType of(int id) {
    return MAP.get(id);
  }
}
