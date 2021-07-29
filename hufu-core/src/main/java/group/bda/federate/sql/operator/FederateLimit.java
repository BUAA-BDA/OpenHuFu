package group.bda.federate.sql.operator;

import java.util.List;

import javax.annotation.Nullable;

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

public class FederateLimit extends SingleRel implements FedSpatialRel {
  public final RexNode offset;
  public final RexNode fetch;

  public FederateLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() == input.getConvention();
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // We do this so we get the limit for free
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public FederateLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new FederateLimit(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (offset != null) {
      implementor.offset = RexLiteral.intValue(offset);
    }
    if (fetch != null) {
      implementor.fetch = RexLiteral.intValue(fetch);
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
