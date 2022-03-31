package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
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

public class OneDBCalc extends Calc implements OneDBRel {

  public OneDBCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program) {
    super(cluster, traitSet, ImmutableList.of(), input, program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = ((RelSubset) getInput()).getBest();
    if (child instanceof OneDBAggCall) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(getInput());
    Header header = implementor.getHeader();
    RexProgram program = getProgram();
    assert header.size() == program.getInputRowType().getFieldCount();
    List<OneDBExpression> calcs =
        OneDBOperator.fromRexNodes(program, implementor.getCurrentOutput());
    implementor.setSelectExps(calcs);
  }

  @Override
  public Calc copy(RelTraitSet traits, RelNode child, RexProgram program) {
    return new OneDBCalc(getCluster(), traits, child, program);
  }

  public static OneDBCalc create(final RelNode input, final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster
            .traitSet()
            .replace(OneDBRel.CONVENTION)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE, () -> RelMdCollation.calc(mq, input, program))
            .replaceIf(
                RelDistributionTraitDef.INSTANCE, () -> RelMdDistribution.calc(mq, input, program));
    return new OneDBCalc(cluster, traitSet, input, program);
  }
}
