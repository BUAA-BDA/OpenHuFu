package com.hufudb.onedb.core.service;

import java.util.HashMap;
import java.util.Map;

public enum Status {
  kOk(0),
  kError(1);

  private static final Map<Integer, Status> MAP = new HashMap<>();

  static {
    for (Status s : values()) {
      MAP.put(s.id, s);
    }
  }

  private final int id;

  Status(int id) {
    this.id = id;
  }

  public static Status of(int status) {
    return values()[status];
  }

  public int id() {
    return id;
  }
}
