package com.hufudb.openhufu.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.core.sql.expression.CalciteConverter;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class OpenHuFuJoin extends Join implements OpenHuFuRel {

  public OpenHuFuJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    final double rowCount = mq.getRowCount(this);
    final double d = (leftRowCount + rightRowCount + rowCount) * 0.5;
    return planner.getCostFactory().makeCost(d, 0, 0);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((OpenHuFuRel) getLeft());
    Plan leftPlan = implementor.getCurrentPlan();
    implementor.setCurrentPlan(null);
    implementor.visitChild((OpenHuFuRel) getRight());
    Plan rightPlan = implementor.getCurrentPlan();
    BinaryPlan joinPlan = new BinaryPlan(leftPlan, rightPlan);
    ImmutableList.Builder<Expression> refBuilder = ImmutableList.builder();
    int idx = 0;
    for (Expression exp: leftPlan.getOutExpressions()) {
      refBuilder.add(ExpressionFactory.createInputRef(idx, exp.getOutType(), exp.getModifier()));
      ++idx;
    }
    for (Expression exp : rightPlan.getOutExpressions()) {
      refBuilder.add(ExpressionFactory.createInputRef(idx, exp.getOutType(), exp.getModifier()));
      ++idx;
    }
    joinPlan.setSelectExps(refBuilder.build());
    JoinCondition joinCondition = CalciteConverter.convert(joinType, joinInfo, joinPlan.getSelectExps());
    joinPlan.setJoinInfo(joinCondition);
    implementor.setCurrentPlan(joinPlan);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new OpenHuFuJoin(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }
}
