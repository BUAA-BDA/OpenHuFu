package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Schema;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.List;
import java.util.stream.Collectors;

/*
 * interface for all expression node add a new expression need to: 1. add an entry in
 * OneDBTranslator.java, 2. add an entry in Interpreter
 */
public interface OneDBExpression {
  static Schema generateHeader(List<OneDBExpression> exps) {
    Schema.Builder builder = Schema.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", exp.getOutType(), exp.getLevel());
    });
    return builder.build();
  }

  static Schema generateHeaderFromProto(List<ExpressionProto> exps) {
    Schema.Builder builder = Schema.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", ColumnType.of(exp.getOutType()), Level.of(exp.getLevel()));
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
    ColumnType outType = ColumnType.of(proto.getOutType());
    Level level = Level.of(proto.getLevel());
    List<OneDBExpression> elements = proto.getInList().stream()
        .map(ele -> OneDBExpression.fromProto(ele)).collect(Collectors.toList());
    return new OneDBOperator(opType, outType, elements, funcType, level);
  }

  static List<OneDBExpression> fromProto(List<ExpressionProto> protos) {
    return protos.stream().map(proto -> fromProto(proto)).collect(Collectors.toList());
  }

  static List<ExpressionProto> toProto(List<OneDBExpression> exps) {
    return exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList());
  }

  ExpressionProto toProto();

  ColumnType getOutType();

  Level getLevel();

  OneDBOpType getOpType();
}
