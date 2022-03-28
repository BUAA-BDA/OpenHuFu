package com.hufudb.onedb.core.sql.expression;

import java.util.List;
import java.util.stream.Collectors;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;

import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

public interface OneDBExpression {
  public ExpressionProto toProto();

  public FieldType getOutType();

  public OneDBOpType getOpType();

  public static Header generateHeader(List<OneDBExpression> exps) {
    Header.Builder builder = Header.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", exp.getOutType());
    });
    return builder.build();
  }

  public static Header generateHeaderFromProto(List<ExpressionProto> exps) {
    Header.Builder builder = Header.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", OneDBExpression.fromProto(exp).getOutType());
    });
    return builder.build();
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    OneDBOpType opType = OneDBOpType.of(proto.getOpType());
    switch (opType) {
      case REF:
        return OneDBReference.fromProto(proto);
      case LITERAL:
        return OneDBLiteral.fromProto(proto);
      case AGG_FUNC:
        return OneDBAggCall.fromProto(proto);
      default:
        break;
    }
    FuncType funcType = FuncType.of(proto.getFunc());
    FieldType outType = FieldType.of(proto.getOutType());
    List<OneDBExpression> elements = proto.getInList().stream().map(ele -> OneDBExpression.fromProto(ele)).collect(Collectors.toList());
    return new OneDBOperator(opType, outType, elements,funcType);
  }
}
