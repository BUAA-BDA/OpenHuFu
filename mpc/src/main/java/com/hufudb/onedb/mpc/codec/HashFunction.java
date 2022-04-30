package com.hufudb.onedb.mpc.codec;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;

public enum HashFunction {
  MD5("MD5", 1, in -> Hashing.md5().hashBytes(in).asBytes()),
  SHA256("SHA256", 2, in -> Hashing.sha256().hashBytes(in).asBytes());

  private static final ImmutableMap<Integer, HashFunction> MAP;

  static {
    final ImmutableMap.Builder<Integer, HashFunction> builder = ImmutableMap.builder();
    for (HashFunction func : values()) {
      builder.put(func.id, func);
    }
    MAP = builder.build();
  }

  private final String name;
  private final int id;
  private final Hash func;

  HashFunction(String name, int id, Hash func) {
    this.name = name;
    this.id = id;
    this.func = func;
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

  public byte[] hash(byte[] in) {
    return func.hash(in);
  }

  public static HashFunction of(int id) {
    return MAP.get(id);
  }

  interface Hash {
    byte[] hash(byte[] in);
  }
}
