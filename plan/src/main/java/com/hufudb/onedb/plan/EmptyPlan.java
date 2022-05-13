package com.hufudb.onedb.plan;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.rewriter.Rewriter;


public class EmptyPlan extends BasePlan {
  List<Expression> output;

  public EmptyPlan(List<Expression> output) {
    this.output = ExpressionUtils.toRefs(output);
  }

  public QueryPlanProto toProto() {
    return QueryPlanProto.newBuilder().setType(getPlanType())
        .addAllSelectExp(output).build();
  }

  public static EmptyPlan fromProto(QueryPlanProto proto) {
    return new EmptyPlan(proto.getSelectExpList());
  }

  @Override
  public PlanType getPlanType() {
    return PlanType.EMPTY;
  }

  @Override
  public List<Expression> getOutExpressions() {
    return output;
  }

  @Override
  public List<ColumnType> getOutTypes() {
    return output.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Modifier getPlanModifier() {
    return ModifierWrapper.dominate(getOutModifiers());
  }

  @Override
  public List<Modifier> getOutModifiers() {
    return output.stream().map(exp -> exp.getModifier()).collect(Collectors.toList());
  }

  @Override
  public Plan rewrite(Rewriter rewriter) {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getOutSchema() {
    return ExpressionUtils.createSchema(output);
  }
}
