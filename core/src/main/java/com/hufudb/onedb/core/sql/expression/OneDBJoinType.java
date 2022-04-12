package com.hufudb.onedb.core.sql.expression;

import org.apache.calcite.rel.core.JoinRelType;

public enum OneDBJoinType {
  NONE,
  INNER,
  LEFT,
  RIGHT,
  OUTER,
  SEMI,
  ANTI;

  public static OneDBJoinType of(int id) {
    return values()[id];
  }

  public static OneDBJoinType of(JoinRelType type) {
    switch (type) {
      case INNER: return INNER;
      case LEFT: return LEFT;
      case RIGHT: return RIGHT;
      case FULL: return OUTER;
      case SEMI: return SEMI;
      case ANTI: return ANTI;
      default: return NONE;
    }
  }
}
