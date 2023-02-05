package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuLimit;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;

import com.hufudb.openhufu.core.sql.rule.OpenHuFuLimitRule.OpenHuFuLimitRuleConfig;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;

public class OpenHuFuLimitRule extends RelRule<OpenHuFuLimitRuleConfig> {

  protected OpenHuFuLimitRule(OpenHuFuLimitRuleConfig config) {
    super(config);
  }


  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (sort.offset == null && sort.fetch == null) {
      return;
    }
    RelTraitSet origTraitSet = sort.getTraitSet();
    RelTraitSet traitSet = origTraitSet.replace(OpenHuFuRel.CONVENTION).simplify();
    RelNode input = sort.getInput();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);
    }
    RelNode x = convert(input, input.getTraitSet().replace(OpenHuFuRel.CONVENTION));
    call.transformTo(new OpenHuFuLimit(sort.getCluster(), traitSet, x, sort.offset, sort.fetch));
  }

  @Value.Immutable
  public interface OpenHuFuLimitRuleConfig extends RelRule.Config {
    OpenHuFuLimitRuleConfig DEFAULT = ImmutableOpenHuFuLimitRuleConfig.builder()
            .operandSupplier(b0 -> b0.operand(Sort.class).anyInputs()).build();

    @Override
    default OpenHuFuLimitRule toRule() {
      return new OpenHuFuLimitRule(this);
    }
  }
}
