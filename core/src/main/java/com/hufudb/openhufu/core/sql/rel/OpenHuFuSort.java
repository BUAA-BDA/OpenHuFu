package com.hufudb.openhufu.core.sql.rel;
import com.hufudb.openhufu.core.sql.expression.CalciteConverter;
import java.util.ArrayList;
import java.util.List;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
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

public class OpenHuFuSort extends Sort implements OpenHuFuRel {

  public OpenHuFuSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation) {
    super(cluster, traitSet, child, collation, null, null);
    assert getConvention() == OpenHuFuRel.CONVENTION;
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
    return new OpenHuFuSort(getCluster(), traitSet, input, collation);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((OpenHuFuRel) getInput());
    List<RelFieldCollation> sortCollations = collation.getFieldCollations();
    List<Collation> fieldOrder = new ArrayList<>();
    if (!sortCollations.isEmpty()) {
      // Construct a series of order clauses from the desired collation
      for (RelFieldCollation fieldCollation : sortCollations) {
        fieldOrder.add(CalciteConverter.convert(fieldCollation));
      }
      implementor.setOrderExps(fieldOrder);
    }
  }
}
