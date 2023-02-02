package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQAggregate;
import com.hufudb.openhufu.core.sql.rel.FQRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class FQAggregateRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalAggregate.class, Convention.NONE, FQRel.CONVENTION, "OneDBAggregateRule")
          .withRuleFactory(FQAggregateRule::new);

  protected FQAggregateRule(Config config) {
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
    return new FQAggregate(
        agg.getCluster(),
        traitSet,
        convert(agg.getInput(), out),
        agg.getGroupSet(),
        agg.getGroupSets(),
        agg.getAggCallList());
  }
}
