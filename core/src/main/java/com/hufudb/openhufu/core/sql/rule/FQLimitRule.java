package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQLimit;
import com.hufudb.openhufu.core.sql.rel.FQRel;

import com.hufudb.openhufu.core.sql.rule.FQLimitRule.FQLimitRuleConfig;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;

public class FQLimitRule extends RelRule<FQLimitRuleConfig> {

  protected FQLimitRule(FQLimitRuleConfig config) {
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
    RelTraitSet traitSet = origTraitSet.replace(FQRel.CONVENTION).simplify();
    RelNode input = sort.getInput();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);
    }
    RelNode x = convert(input, input.getTraitSet().replace(FQRel.CONVENTION));
    call.transformTo(new FQLimit(sort.getCluster(), traitSet, x, sort.offset, sort.fetch));
  }

  @Value.Immutable
  public interface FQLimitRuleConfig extends RelRule.Config {
    FQLimitRuleConfig DEFAULT = ImmutableFQLimitRuleConfig.builder()
            .operandSupplier(b0 -> b0.operand(Sort.class).anyInputs()).build();

    @Override
    default FQLimitRule toRule() {
      return new FQLimitRule(this);
    }
  }
}
