package tk.onedb.core.sql.expression;

import java.util.List;
import java.util.stream.Collectors;

import tk.onedb.core.data.FieldType;
import tk.onedb.core.data.Header;
import tk.onedb.core.sql.expression.OneDBOperator.FuncType;
import tk.onedb.rpc.OneDBCommon.ExpressionProto;

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
