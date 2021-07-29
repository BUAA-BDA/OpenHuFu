package group.bda.federate.sql.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

public class FederateAggregate extends Aggregate implements FedSpatialRel {
  private boolean hasAvg = false;

  public FederateAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    for (AggregateCall call : aggCalls) {
      if (call.getAggregation().getKind().equals(SqlKind.AVG)) {
        hasAvg = true;
      }
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (hasAvg) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.5);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.addAggregate(aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new FederateAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }
}
