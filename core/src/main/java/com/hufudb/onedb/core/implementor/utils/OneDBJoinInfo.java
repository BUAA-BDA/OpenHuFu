package com.hufudb.onedb.core.implementor.utils;

import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBJoinType;
import com.hufudb.onedb.rpc.OneDBCommon.JoinConditionProto;
import java.util.List;

public class OneDBJoinInfo {
  final OneDBJoinType type;
  final List<Integer> leftKeys;
  final List<Integer> rightKeys;
  final List<OneDBExpression> conditions;
  final Level level;
  final int leftSize;
  boolean isLeft; // only for owner

  public OneDBJoinInfo(OneDBJoinType type, List<Integer> leftKeys, List<Integer> rightKeys,
      List<OneDBExpression> conditions, Level level, int leftSize) {
    this.type = type;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.conditions = conditions;
    this.level = level;
    this.leftSize = leftSize;
  }

  public JoinConditionProto toProto(boolean isLeft) {
    return JoinConditionProto.newBuilder().addAllLeftKey(leftKeys).addAllRightKey(rightKeys)
        .addAllCondition(OneDBExpression.toProto(conditions)).setLevel(level.getId())
        .setLeftSize(leftSize).setIsLeft(isLeft).build();
  }

  public static OneDBJoinInfo fromProto(JoinConditionProto proto) {
    OneDBJoinInfo joinInfo = new OneDBJoinInfo(OneDBJoinType.of(proto.getType()), proto.getLeftKeyList(),
        proto.getRightKeyList(), OneDBExpression.fromProto(proto.getConditionList()),
        Level.of(proto.getLevel()), proto.getLeftSize());
    joinInfo.isLeft = proto.getIsLeft();
    return joinInfo;
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

  public Level getLevel() {
    return level;
  }

  public boolean isLeft() {
    return isLeft;
  }
}
