package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBLimit;
import com.hufudb.onedb.core.sql.rel.OneDBRel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.rel.core.Sort;

public class OneDBLimitRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new OneDBLimitRule();

  protected OneDBLimitRule() {
    super(operand(Sort.class, any()), "OneDBLimitRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (sort.offset == null && sort.fetch == null) {
      return;
    }

    RelTraitSet origTraitSet = sort.getTraitSet();
    RelTraitSet traitSet = origTraitSet.replace(OneDBRel.CONVENTION).simplify();

    RelNode input = sort.getInput();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);
    }
    RelNode x = convert(input, input.getTraitSet().replace(OneDBRel.CONVENTION));
    call.transformTo(new OneDBLimit(sort.getCluster(), traitSet, x, sort.offset, sort.fetch));
  }

}
