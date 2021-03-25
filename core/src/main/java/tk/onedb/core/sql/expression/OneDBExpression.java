package tk.onedb.core.sql.expression;

import tk.onedb.rpc.OneDBCommon.ExpressionProto;

public interface OneDBExpression {
  public ExpressionProto toProto();
}
