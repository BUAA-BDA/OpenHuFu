package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBJoin;
import com.hufudb.onedb.core.sql.rel.OneDBRel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

public class OneDBJoinRule extends ConverterRule {
  protected static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalJoin.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBJoinRule")
      .withRuleFactory(OneDBJoinRule::new);

  protected OneDBJoinRule(Config config) {
    super(config);
  }

  // todo: extend this function
  @Override
  public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    final RelTraitSet traitSet = join.getTraitSet().replace(OneDBRel.CONVENTION);
    return new OneDBJoin(join.getCluster(), traitSet, join.getInput(0), join.getInput(1), join.getCondition(),
        join.getVariablesSet(), join.getJoinType());
  }
}
