package com.hufudb.openhufu.expression;

import java.util.List;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;

/**
 * Convert an Expression into String
 */
public interface Translator {
  void setInput(List<String> inputs);
  String translate(Expression exp);
}
