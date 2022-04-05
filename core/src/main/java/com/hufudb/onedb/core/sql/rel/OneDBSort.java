package com.hufudb.onedb.core.sql.rel;
import java.util.ArrayList;
import java.util.List;

import com.hufudb.onedb.core.data.query.sort.OneDBOrder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class OneDBSort extends Sort implements OneDBRel {

  public OneDBSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation) {
    super(cluster, traitSet, child, collation, null, null);
    assert getConvention() == OneDBRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (!collation.getFieldCollations().isEmpty()) {
      return cost.multiplyBy(0.05);
    } else {
      return cost;
    }
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new OneDBSort(getCluster(), traitSet, input, collation);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(getInput());
    List<RelFieldCollation> sortCollations = collation.getFieldCollations();
    List<String> fieldOrder = new ArrayList<>();
    if (!sortCollations.isEmpty()) {
      // Construct a series of order clauses from the desired collation
      for (RelFieldCollation fieldCollation : sortCollations) {
        fieldOrder.add(OneDBOrder.fromCollation(fieldCollation).toString());
      }
      implementor.setOrderExps(fieldOrder);
    }
  }
}
