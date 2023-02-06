package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuJoin;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

public class OpenHuFuJoinRule extends ConverterRule {
  protected static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(LogicalJoin.class, Convention.NONE, OpenHuFuRel.CONVENTION, "OpenHuFuJoinRule")
          .withRuleFactory(OpenHuFuJoinRule::new);

  protected OpenHuFuJoinRule(Config config) {
    super(config);
  }

  // todo: extend this function for left / right / outer join
  @Override
  public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    final RelTraitSet traitSet = join.getTraitSet().replace(OpenHuFuRel.CONVENTION);
    RelNode left = join.getInput(0);
    RelNode right = join.getInput(1);
    left = convert(left, left.getTraitSet().replace(OpenHuFuRel.CONVENTION));
    right = convert(right, right.getTraitSet().replace(OpenHuFuRel.CONVENTION));
    return new OpenHuFuJoin(
        join.getCluster(),
        traitSet,
        left,
        right,
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
