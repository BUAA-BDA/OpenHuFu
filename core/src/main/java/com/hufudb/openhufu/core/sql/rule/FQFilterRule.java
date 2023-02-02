package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQFilter;
import com.hufudb.openhufu.core.sql.rel.FQRel;
import com.hufudb.openhufu.core.sql.rel.FQTableScan;
import com.hufudb.openhufu.core.sql.rule.FQFilterRule.FQFilterRuleConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.immutables.value.Value;

public class FQFilterRule extends RelRule<FQFilterRuleConfig> {
  protected FQFilterRule(FQFilterRuleConfig config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    FQTableScan scan = call.rel(1);
    if (filter.getTraitSet().contains(Convention.NONE)) {
      final RelNode converted = convert(filter, scan);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  RelNode convert(LogicalFilter filter, FQTableScan scan) {
    final RelTraitSet traitSet = filter.getTraitSet().replace(FQRel.CONVENTION);
    return new FQFilter(
        filter.getCluster(),
        traitSet,
        convert(filter.getInput(), FQRel.CONVENTION),
        filter.getCondition());
  }

  @Value.Immutable
  public interface FQFilterRuleConfig extends RelRule.Config {
    FQFilterRuleConfig DEFAULT =
        ImmutableFQFilterRuleConfig.builder()
            .operandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(b1 -> b1.operand(FQTableScan.class).noInputs()))
            .build();

    @Override
    default FQFilterRule toRule() {
      return new FQFilterRule(this);
    }
  }
}
