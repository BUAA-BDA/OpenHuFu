package com.hufudb.openhufu.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.core.sql.expression.CalciteConverter;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

public class FQCalc extends Calc implements FQRel {

  public FQCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program) {
    super(cluster, traitSet, ImmutableList.of(), input, program);
  }

  public static FQCalc create(final RelNode input, final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet().replace(FQRel.CONVENTION)
        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> RelMdCollation.calc(mq, input, program))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.calc(mq, input, program));
    return new FQCalc(cluster, traitSet, input, program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = ((RelSubset) getInput()).getBest();
    if (child instanceof FQAggregate) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild((FQRel) getInput());
    RexProgram program = getProgram();
    List<Expression> calcs =
        CalciteConverter.convert(program, implementor.getCurrentOutput());
    if (!implementor.getAggExps().isEmpty()) {
      implementor.setAggExps(calcs);
    } else {
      implementor.setSelectExps(calcs);
    }
  }

  @Override
  public Calc copy(RelTraitSet traits, RelNode child, RexProgram program) {
    return new FQCalc(getCluster(), traits, child, program);
  }
}
