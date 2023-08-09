package com.hufudb.openhufu.expression;

import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;

import java.util.List;

/**
 * Convert an Expression into String
 */
public interface Translator {
  void setInput(List<String> inputs);
  String translate(Expression exp);
}
