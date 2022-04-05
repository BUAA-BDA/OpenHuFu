package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBLimit;
import com.hufudb.onedb.core.sql.rel.OneDBRel;

import com.hufudb.onedb.core.sql.rel.OneDBToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import org.immutables.value.Value;

public class OneDBLimitRule extends RelRule<OneDBLimitRule.OneDBLimitRuleConfig> {

  protected OneDBLimitRule(OneDBLimitRuleConfig config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final EnumerableLimit limit = call.rel(0);
    final RelNode converted = convert(limit);
    if (converted != null) {
      call.transformTo(converted);
    }

  }


  public RelNode convert(RelNode relNode) {
    final EnumerableLimit limit = (EnumerableLimit) relNode;
    final RelTraitSet traitSet = limit.getTraitSet().replace(OneDBRel.CONVENTION);
    return new OneDBLimit(
            limit.getCluster(),
            traitSet,
            convert(limit.getInput(), OneDBLimit.CONVENTION),
            limit.offset,
            limit.fetch);
  }

  @Value.Immutable
  public interface OneDBLimitRuleConfig extends RelRule.Config {
    OneDBLimitRule.OneDBLimitRuleConfig DEFAULT = ImmutableOneDBLimitRuleConfig.builder()
            .operandSupplier(
                    b0 ->
                            b0.operand(EnumerableLimit.class)
                                    .oneInput(b1 -> b1.operand(OneDBToEnumerableConverter.class).anyInputs()))
            .build();

    @Override
    default OneDBLimitRule toRule() {
      return new OneDBLimitRule(this);
    }
  }
}
