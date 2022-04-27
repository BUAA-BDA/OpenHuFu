package com.hufudb.onedb.core.query.implementor.utils;

import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBJoinType;
import java.util.List;

public class OneDBJoinInfo {
  OneDBJoinType type;
  List<Integer> leftKeys;
  List<Integer> rightKeys;
  List<OneDBExpression> conditions;

  public OneDBJoinInfo(OneDBJoinType type, List<Integer> leftKeys, List<Integer> rightKeys,
      List<OneDBExpression> conditions) {
    this.type = type;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.conditions = conditions;
  }

  public OneDBJoinType getType() {
    return type;
  }

  public List<Integer> getLeftKeys() {
    return leftKeys;
  }

  public List<Integer> getRightKeys() {
    return rightKeys;
  }

  public List<OneDBExpression> getConditions() {
    return conditions;
  }
}
