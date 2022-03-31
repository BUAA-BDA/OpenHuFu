package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
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

public class OneDBJoin extends Join implements OneDBRel {

  public OneDBJoin(
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
    implementor.visitChild(getLeft(), getRight());
    implementor.setJoinCondition(analyzeCondition());
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new OneDBJoin(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }
}
