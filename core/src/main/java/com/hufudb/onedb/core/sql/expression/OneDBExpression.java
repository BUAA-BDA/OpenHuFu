package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.List;
import java.util.stream.Collectors;

/*
 * interface for all expression node
 * add a new expression need to:
 * 1. add an entry in OneDBTranslator.java,
 * 2. add an entry in Interpreter
 */
public interface OneDBExpression {
  static Header generateHeader(List<OneDBExpression> exps) {
    Header.Builder builder = Header.newBuilder();
    exps.stream()
        .forEach(
            exp -> {
              builder.add("", exp.getOutType());
            });
    return builder.build();
  }

  static Header generateHeaderFromProto(List<ExpressionProto> exps) {
    Header.Builder builder = Header.newBuilder();
    exps.stream()
        .forEach(
            exp -> {
              builder.add("", FieldType.of(exp.getOutType()));
            });
    return builder.build();
  }

  static OneDBExpression fromProto(ExpressionProto proto) {
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
    List<OneDBExpression> elements =
        proto.getInList().stream()
            .map(ele -> OneDBExpression.fromProto(ele))
            .collect(Collectors.toList());
    return new OneDBOperator(opType, outType, elements, funcType);
  }

  static List<OneDBExpression> fromProto(List<ExpressionProto> protos) {
    return protos.stream().map(proto -> fromProto(proto)).collect(Collectors.toList());
  }

  static List<ExpressionProto> toProto(List<OneDBExpression> exps) {
    return exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList());
  }

  ExpressionProto toProto();

  FieldType getOutType();

  OneDBOpType getOpType();
}
