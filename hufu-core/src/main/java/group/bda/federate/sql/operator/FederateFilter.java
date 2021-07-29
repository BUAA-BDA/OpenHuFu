package group.bda.federate.sql.operator;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import group.bda.federate.data.Header;
import group.bda.federate.sql.expression.FedSpatialExpression;

public class FederateFilter extends Filter implements FedSpatialRel {
  private final Header header;


  public FederateFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition, Header header) {
    super(cluster, traitSet, child, condition);
    this.header = header;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Header header = implementor.getHeader();
    assert header.size() == getInput().getRowType().getFieldCount();
    implementor.setFilterExp(FedSpatialExpression.create(header, getCondition()));
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new FederateFilter(getCluster(), traitSet, input, condition, this.header);
  }
}
