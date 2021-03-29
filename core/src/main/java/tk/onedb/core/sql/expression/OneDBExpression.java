package tk.onedb.core.sql.expression;

import java.util.List;

import tk.onedb.core.data.FieldType;
import tk.onedb.core.data.Header;
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
}
