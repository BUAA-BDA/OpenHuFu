package com.hufudb.onedb.core.sql.expression;

public enum OneDBOpType {
  REF, // for input ref
  LITERAL, // for literal
  // for binary
  PLUS,
  MINUS,
  TIMES,
  DIVIDE,
  MOD,
  GT,
  GE,
  LT,
  LE,
  EQ,
  NE,
  AND,
  OR,
  // for unary
  AS,
  NOT,
  PLUS_PRE,
  MINUS_PRE,
  IS_NULL,
  IS_NOT_NULL,
  // for case
  CASE,
  // for functions
  SCALAR_FUNC,
  AGG_FUNC;

  public static OneDBOpType of(int id) {
    return OneDBOpType.values()[id];
  }
}
