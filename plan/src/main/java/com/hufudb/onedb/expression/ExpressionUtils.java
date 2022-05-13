package com.hufudb.onedb.expression;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionUtils {
  private final static Logger LOG = LoggerFactory.getLogger(ExpressionUtils.class);

  public static Schema createSchema(List<Expression> exps) {
    Schema.Builder builder = Schema.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", exp.getOutType(), exp.getModifier());
    });
    return builder.build();
  }

  public static Expression createRef(ColumnType type, Modifier modifier, int ref) {
    return Expression.newBuilder().setOpType(OperatorType.REF).setOutType(type)
        .setModifier(modifier).setI32(ref).build();
  }

  public static List<Expression> toRefs(List<Expression> exps) {
    ImmutableList.Builder<Expression> ans = ImmutableList.builder();
    for (int i = 0; i < exps.size(); ++i) {
      ans.add(createRef(exps.get(i).getOutType(), exps.get(i).getModifier(), i));
    }
    return ans.build();
  }

  public static List<Integer> getAggInputs(Expression agg) {
    assert agg.getOpType().equals(OperatorType.AGG_FUNC);
    return agg.getInList().stream().map(exp -> exp.getI32()).collect(Collectors.toList());
  }
}
