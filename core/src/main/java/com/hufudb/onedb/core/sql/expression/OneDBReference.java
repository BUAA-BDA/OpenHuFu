package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Schema;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.ArrayList;
import java.util.List;

public class OneDBReference implements OneDBExpression {
  ColumnType type;
  Level level;
  int idx;

  public OneDBReference(ColumnType type, Level level, int idx) {
    this.type = type;
    this.level = level;
    this.idx = idx;
  }

  public static List<OneDBExpression> fromHeader(Schema header) {
    List<OneDBExpression> exps = new ArrayList<>();
    for (int i = 0; i < header.size(); ++i) {
      exps.add(new OneDBReference(header.getType(i), header.getLevel(i), i));
    }
    return exps;
  }

  public static OneDBExpression fromIndex(Schema header, int i) {
    return new OneDBReference(header.getType(i), header.getLevel(i), i);
  }

  public static OneDBExpression fromIndex(ColumnType type, Level level, int i) {
    return new OneDBReference(type, level, i);
  }

  public static List<OneDBExpression> fromExps(List<OneDBExpression> exps) {
    List<OneDBExpression> nexps = new ArrayList<>();
    for (int i = 0; i < exps.size(); ++i) {
      nexps.add(new OneDBReference(exps.get(i).getOutType(), exps.get(i).getLevel(), i));
    }
    return nexps;
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    ColumnType type = ColumnType.of(proto.getOutType());
    Level level = Level.of(proto.getLevel());
    int idx = proto.getI32();
    return new OneDBReference(type, level, idx);
  }

  @Override
  public ExpressionProto toProto() {
    return ExpressionProto.newBuilder()
        .setOpType(OneDBOpType.REF.ordinal())
        .setOutType(type.ordinal())
        .setLevel(level.getId())
        .setI32(idx)
        .build();
  }

  @Override
  public ColumnType getOutType() {
    return type;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.REF;
  }

  @Override
  public Level getLevel() {
    return level;
  }

  public int getIdx() {
    return idx;
  }
}
