package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.ArrayList;
import java.util.List;

public class OneDBReference implements OneDBExpression {
  FieldType type;
  Level level;
  int idx;

  public OneDBReference(FieldType type, Level level, int idx) {
    this.type = type;
    this.level = level;
    this.idx = idx;
  }

  public static List<OneDBExpression> fromHeader(Header header) {
    List<OneDBExpression> exps = new ArrayList<>();
    for (int i = 0; i < header.size(); ++i) {
      exps.add(new OneDBReference(header.getType(i), header.getLevel(i), i));
    }
    return exps;
  }

  public static OneDBExpression fromIndex(Header header, int i) {
    return new OneDBReference(header.getType(i), header.getLevel(i), i);
  }

  public static OneDBExpression fromIndex(FieldType type, Level level, int i) {
    return new OneDBReference(type, level, i);
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    FieldType type = FieldType.of(proto.getOutType());
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
  public FieldType getOutType() {
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
