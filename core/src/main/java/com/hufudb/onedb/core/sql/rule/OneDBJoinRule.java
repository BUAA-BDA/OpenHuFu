package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBJoin;
import com.hufudb.onedb.core.sql.rel.OneDBRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

public class OneDBJoinRule extends ConverterRule {
  protected static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(LogicalJoin.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBJoinRule")
          .withRuleFactory(OneDBJoinRule::new);

  protected OneDBJoinRule(Config config) {
    super(config);
  }

  // todo: extend this function for left / right / outer join
  @Override
  public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    final RelTraitSet traitSet = join.getTraitSet().replace(OneDBRel.CONVENTION);
    RelNode left = join.getInput(0);
    RelNode right = join.getInput(1);
    left = convert(left, left.getTraitSet().replace(OneDBRel.CONVENTION));
    right = convert(right, right.getTraitSet().replace(OneDBRel.CONVENTION));
    return new OneDBJoin(
        join.getCluster(),
        traitSet,
        left,
        right,
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
