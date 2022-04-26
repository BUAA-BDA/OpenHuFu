package com.hufudb.onedb.mpc;

import com.google.common.collect.ImmutableMap;

public enum ProtocolType {
  ERROR("ERROR", 0),
  PK_OT("PUBLIC_KEY_BASED_OT", 10),
  BEAVER_TRIPLE("BEAVER_TRIPLE", 20),
  GMW("GMW", 30);

  private static final ImmutableMap<Integer, ProtocolType> MAP;

  static {
    final ImmutableMap.Builder<Integer, ProtocolType> builder = ImmutableMap.builder();
    for (ProtocolType type : values()) {
      builder.put(type.id, type);
    }
    MAP = builder.build();
  }

  private final String name;
  private final int id;

  ProtocolType(String name, int id) {
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

  public static ProtocolType type(int id) {
    return MAP.get(id);
  }
}
