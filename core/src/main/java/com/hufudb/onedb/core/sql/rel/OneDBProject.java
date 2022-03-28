package com.hufudb.onedb.core.sql.rel;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class OneDBProject extends Project implements OneDBRel {
  public OneDBProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    assert getConvention() == OneDBProject.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new OneDBProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(getInput());
    List<OneDBExpression> exps = OneDBReference.fromHeader(implementor.getCurrentOutput(), getProjects());
    implementor.setSelectExps(exps);
  }
}
