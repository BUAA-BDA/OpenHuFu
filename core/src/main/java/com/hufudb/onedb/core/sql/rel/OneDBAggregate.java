package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.sql.expression.CalciteConverter;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.UnaryPlan;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
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

public class OneDBAggregate extends Aggregate implements OneDBRel {
  public OneDBAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((OneDBRel) getInput());
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
    return new OneDBAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }
}
