package com.hufudb.onedb.core.sql.expression;

import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.TypeConverter;

import org.apache.calcite.rex.RexInputRef;

import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

public class OneDBReference implements OneDBExpression {
  FieldType type;
  int idx;

  public OneDBReference(FieldType type, int idx) {
    this.type = type;
    this.idx = idx;
  }

  public static List<OneDBExpression> fromHeader(Header header) {
    List<OneDBExpression> exps = new ArrayList<>();
    for (int i = 0; i < header.size(); ++i) {
      exps.add(new OneDBReference(header.getType(i), i));
    }
    return exps;
  }

  public static OneDBExpression fromIndex(FieldType type, int i) {
    return new OneDBReference(type, i);
  }

  public static OneDBExpression fromInputRef(RexInputRef ref) {
    return new OneDBReference(TypeConverter.convert2OneDBType(ref.getType().getSqlTypeName()), ref.getIndex());
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    FieldType type = FieldType.of(proto.getOutType());
    int idx = proto.getRef();
    return new OneDBReference(type, idx);
  }

  @Override
  public ExpressionProto toProto() {
    return ExpressionProto.newBuilder().setOpType(OneDBOpType.REF.ordinal()).setOutType(type.ordinal()).setRef(idx).build();
  }

  @Override
  public FieldType getOutType() {
    return type;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.REF;
  }

  public int getIdx() {
    return idx;
  }
}
