package com.hufudb.onedb.mpc;

import com.google.common.collect.ImmutableMap;

/*
 * Protocols supported by OneDB
 * Don't change the id of existing protocols (relate to Level.java)
 */
public enum ProtocolType {
  UNKNOWN("UNKNOWN", -1, true),
  PLAINTEXT("PLAINTEXT", 0, false),
  BOARDCAST("BOARDCAST", 1, true),
  STREAM("STREAM", 2, true),
  PK_OT("PUBLIC_KEY_BASED_OT", 10, true),
  BEAVER_TRIPLE("BEAVER_TRIPLE", 20, true),
  GMW("GMW", 100, true),
  HASH_PSI("PSI", 200, true);

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
  private final boolean multiparty;

  ProtocolType(String name, int id, boolean multiparty) {
    this.name = name;
    this.id = id;
    this.multiparty = multiparty;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public boolean isMultiParty() {
    return multiparty;
  }

  public String toString() {
    return name;
  }

  public static ProtocolType of(int id) {
    return MAP.get(id);
  }
}
