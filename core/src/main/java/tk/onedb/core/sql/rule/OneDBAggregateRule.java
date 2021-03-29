package tk.onedb.core.sql.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

import tk.onedb.core.sql.rel.OneDBAggregate;
import tk.onedb.core.sql.rel.OneDBRel;

public class OneDBAggregateRule extends ConverterRule {
  static final ConverterRule.Config DEFAULT_CONFIG = ConverterRule.Config.INSTANCE
      .withConversion(LogicalAggregate.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBAggregateRule")
      .withRuleFactory(OneDBAggregateRule::new);

  protected OneDBAggregateRule(Config config) {
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
    return new OneDBAggregate(agg.getCluster(), traitSet, convert(agg.getInput(), out), agg.getGroupSet(),
        agg.getGroupSets(), agg.getAggCallList());
  }
}
