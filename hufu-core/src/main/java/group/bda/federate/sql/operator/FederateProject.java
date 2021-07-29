package group.bda.federate.sql.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import group.bda.federate.sql.expression.FedSpatialExpressions;

public class FederateProject extends Project implements FedSpatialRel {

  public FederateProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    assert getConvention() == FederateProject.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new FederateProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    FedSpatialExpressions exps = FedSpatialExpressions.create(implementor.getHeader(), getProjects());
    implementor.setSelectExps(exps);
  }
}
