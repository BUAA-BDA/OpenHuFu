package com.hufudb.openhufu.core.sql.rel;


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.List;


public class FQLimit extends SingleRel implements FQRel {
  public final RexNode offset;
  public final RexNode fetch;

  public FQLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() == input.getConvention();
  }

  @Override
  public @Nullable
  RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public FQLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new FQLimit(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((FQRel) getInput());
    if (offset != null) {
      implementor.setOffset(RexLiteral.intValue(offset));
    }
    if (fetch != null) {
      implementor.setFetch(RexLiteral.intValue(fetch));
    }
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("fetch", fetch, fetch != null);
    return pw;
  }
}
