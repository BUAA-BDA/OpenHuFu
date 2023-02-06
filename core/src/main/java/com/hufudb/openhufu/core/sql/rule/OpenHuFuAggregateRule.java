package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuAggregate;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class OpenHuFuAggregateRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalAggregate.class, Convention.NONE, OpenHuFuRel.CONVENTION, "OpenHuFuAggregateRule")
          .withRuleFactory(OpenHuFuAggregateRule::new);

  protected OpenHuFuAggregateRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final LogicalAggregate agg = (LogicalAggregate) relNode;
    final RelTraitSet traitSet = agg.getTraitSet().replace(out);
    return new OpenHuFuAggregate(
        agg.getCluster(),
        traitSet,
        convert(agg.getInput(), out),
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList());
  }
}
