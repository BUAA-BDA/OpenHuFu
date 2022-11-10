package com.hufudb.onedb.mpc.bristol;

import java.io.InputStream;
import com.google.common.collect.ImmutableMap;

public enum CircuitType {
  ADD_32("ADD_32", "bristol/adder_32bit.txt", 1),
  ADD_64("ADD_64", "bristol/adder_64bit.txt", 2),
  LT_32("LT_32", "bristol/comparator_32bit_signed_lt.txt", 10),
  LTE_32("LTE_32", "bristol/comparator_32bit_signed_lteq.txt", 11);

  private static final ImmutableMap<Integer, CircuitType> MAP;

  private final String name;
  private BristolFile bristol;
  private final int id;

  static {
    final ImmutableMap.Builder<Integer, CircuitType> builder = ImmutableMap.builder();
    for (CircuitType type : values()) {
      builder.put(type.id, type);
    }
    MAP = builder.build();
  }

  CircuitType(String name, String path, int id) {
    this.name = name;
    InputStream inputStream = BristolFile.class.getClassLoader().getResourceAsStream(path);
    this.bristol = BristolFile.fromStream(inputStream);
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

  public BristolFile getBristol() {
    return bristol;
  }

  public static CircuitType of(int id) {
    return MAP.get(id);
  }
}
