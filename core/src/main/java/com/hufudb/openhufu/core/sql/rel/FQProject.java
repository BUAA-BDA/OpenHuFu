package com.hufudb.openhufu.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.core.sql.expression.CalciteConverter;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class FQProject extends Project implements FQRel {
  public FQProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    assert getConvention() == CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new FQProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((FQRel) getInput());
    List<Expression> exps =
        CalciteConverter.convert(getProjects(), implementor.getCurrentOutput());
    List<Expression> aggs = implementor.getAggExps();
    if (!aggs.isEmpty()) {
      implementor.setAggExps(exps);
    } else {
      implementor.setSelectExps(exps);
    }
  }
}
