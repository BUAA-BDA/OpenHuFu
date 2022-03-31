package com.hufudb.onedb.core.sql.implementor.utils;

import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProtoOrBuilder;
import java.util.List;

public class OneDBJoinInfo {
  List<Integer> leftKeys;
  List<Integer> rightKeys;
  List<ExpressionProto> conditions;

  public OneDBJoinInfo(OneDBQueryProtoOrBuilder proto) {
    this.leftKeys = proto.getLeftKeyList();
    this.rightKeys = proto.getRightKeyList();
    this.conditions = proto.getJoinCondList();
  }

  public List<Integer> getLeftKeys() {
    return leftKeys;
  }

  public List<Integer> getRightKeys() {
    return rightKeys;
  }

  public List<ExpressionProto> getConditions() {
    return conditions;
  }
}
