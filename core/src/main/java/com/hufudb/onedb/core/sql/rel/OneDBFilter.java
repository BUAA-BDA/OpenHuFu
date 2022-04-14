package com.hufudb.onedb.core.sql.rel;

import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class OneDBFilter extends Filter implements OneDBRel {
  public OneDBFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((OneDBRel) getInput());
    implementor.addFilterExps(OneDBOperator.fromRexNode(condition, implementor.getCurrentOutput()));
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new OneDBFilter(getCluster(), traitSet, input, condition);
  }
}
