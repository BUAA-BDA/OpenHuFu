package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuFilter;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.immutables.value.Value;

public class OpenHuFuFilterRule extends RelRule<OpenHuFuFilterRule.OpenHuFuFilterRuleConfig> {
  protected OpenHuFuFilterRule(OpenHuFuFilterRuleConfig config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    OpenHuFuTableScan scan = call.rel(1);
    if (filter.getTraitSet().contains(Convention.NONE)) {
      final RelNode converted = convert(filter, scan);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  RelNode convert(LogicalFilter filter, OpenHuFuTableScan scan) {
    final RelTraitSet traitSet = filter.getTraitSet().replace(OpenHuFuRel.CONVENTION);
    return new OpenHuFuFilter(
        filter.getCluster(),
        traitSet,
        convert(filter.getInput(), OpenHuFuRel.CONVENTION),
        filter.getCondition());
  }

  @Value.Immutable
  public interface OpenHuFuFilterRuleConfig extends RelRule.Config {
    OpenHuFuFilterRuleConfig DEFAULT =
        ImmutableOpenHuFuFilterRuleConfig.builder()
            .operandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(b1 -> b1.operand(OpenHuFuTableScan.class).noInputs()))
            .build();

    @Override
    default OpenHuFuFilterRule toRule() {
      return new OpenHuFuFilterRule(this);
    }
  }
}
