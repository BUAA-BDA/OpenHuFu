package com.hufudb.onedb.expression;

import com.hufudb.onedb.proto.OneDBPlan.Expression;

/**
 * Convert an Expression into String
 */
public interface Translator {
  String translate(Expression exp);
}
