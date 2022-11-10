package com.hufudb.onedb.expression;

import java.util.List;
import com.hufudb.onedb.proto.OneDBPlan.Expression;

/**
 * Convert an Expression into String
 */
public interface Translator {
  void setInput(List<String> inputs);
  String translate(Expression exp);
}
