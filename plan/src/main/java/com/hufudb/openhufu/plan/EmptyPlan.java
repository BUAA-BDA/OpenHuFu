package com.hufudb.openhufu.plan;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.expression.ExpressionUtils;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import com.hufudb.openhufu.rewriter.Rewriter;


public class EmptyPlan extends BasePlan {
  List<Expression> output;

  public EmptyPlan(List<Expression> output) {
    this.output = ExpressionFactory.createInputRef(output);
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
