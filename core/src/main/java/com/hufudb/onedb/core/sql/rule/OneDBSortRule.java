package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBRel;
import com.hufudb.onedb.core.sql.rel.OneDBSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;


public class OneDBSortRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
          Config.INSTANCE
                  .withConversion(
                          Sort.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBSortRule")
                  .withRuleFactory(OneDBSortRule::new);

  protected OneDBSortRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final Sort sort = (Sort) relNode;
    final RelTraitSet traitSet = sort.getTraitSet().replace(OneDBRel.CONVENTION);
    final RelNode input = sort.getInput();
    return new OneDBSort(
            relNode.getCluster(),
            traitSet,
            convert(input, input.getTraitSet().replace(OneDBRel.CONVENTION)),
            sort.getCollation());
  }
}
