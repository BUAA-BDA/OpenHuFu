package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQRel;
import com.hufudb.openhufu.core.sql.rel.FQSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;


public class FQSortRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
          Config.INSTANCE
                  .withConversion(
                          Sort.class, Convention.NONE, FQRel.CONVENTION, "OneDBSortRule")
                  .withRuleFactory(FQSortRule::new);

  protected FQSortRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final Sort sort = (Sort) relNode;
    if (sort.offset != null || sort.fetch != null) {
      return null;
    }
    final RelTraitSet traitSet = sort.getTraitSet().replace(FQRel.CONVENTION);
    final RelNode input = sort.getInput();
    return new FQSort(
            relNode.getCluster(),
            traitSet,
            convert(input, input.getTraitSet().replace(FQRel.CONVENTION)),
            sort.getCollation());
  }
}
