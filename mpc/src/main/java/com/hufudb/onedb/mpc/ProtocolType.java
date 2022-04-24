package com.hufudb.onedb.mpc;

import java.util.HashMap;
import java.util.Map;

public enum ProtocolType {
  PK_OT("PUBLIC_KEY_BASED_OT", 10),
  BEAVER_TRIPLE("BEAVER_TRIPLE", 20),
  GMW("GMW", 30);

  private static final Map<String, ProtocolType> MAP = new HashMap<>();

  static {
    for (ProtocolType type : values()) {
      MAP.put(type.name, type);
    }
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
}
