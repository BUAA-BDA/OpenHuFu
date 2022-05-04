package com.hufudb.onedb.mpc;

import com.google.common.collect.ImmutableMap;

/*
 * Protocols supported by OneDB
 * Don't change the id of existing protocols (relate to Level.java)
 */
public enum ProtocolType {
  UNKNOWN("UNKNOWN", -1, false),
  PLAINTEXT("PLAINTEXT", 0, true),
  PK_OT("PUBLIC_KEY_BASED_OT", 10, false),
  BEAVER_TRIPLE("BEAVER_TRIPLE", 20, false),
  GMW("GMW", 100, false),
  HASH_PSI("PSI", 200, false);

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
  private final boolean plaintext;

  ProtocolType(String name, int id, boolean plaintext) {
    this.name = name;
    this.id = id;
    this.plaintext = plaintext;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public boolean isPlaintext() {
    return plaintext;
  }

  public String toString() {
    return name;
  }

  public static ProtocolType of(int id) {
    return MAP.get(id);
  }
}
