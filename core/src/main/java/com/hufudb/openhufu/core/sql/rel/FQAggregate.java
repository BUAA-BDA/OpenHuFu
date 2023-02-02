package com.hufudb.openhufu.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.core.sql.expression.CalciteConverter;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

public class FQAggregate extends Aggregate implements FQRel {
  public FQAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((FQRel) getInput());
    List<Integer> groups = new ArrayList<>(getGroupSet().asList());
    List<Expression> aggs = CalciteConverter.convert(groups, aggCalls, implementor.getCurrentOutput());
    if (!implementor.getAggExps().isEmpty()) {
      UnaryPlan plan = new UnaryPlan(implementor.getCurrentPlan());
      plan.setSelectExps(ExpressionFactory.createInputRef(implementor.getAggExps()));
      plan.setAggExps(aggs);
      implementor.setCurrentPlan(plan);
    } else {
      implementor.setAggExps(aggs);
    }
    implementor.setGroupSet(groups);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new FQAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }
}
