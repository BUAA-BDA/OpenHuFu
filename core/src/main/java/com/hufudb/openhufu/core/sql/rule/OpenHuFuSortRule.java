package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;


public class OpenHuFuSortRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
          Config.INSTANCE
                  .withConversion(
                          Sort.class, Convention.NONE, OpenHuFuRel.CONVENTION, "OpenHuFuSortRule")
                  .withRuleFactory(OpenHuFuSortRule::new);

  protected OpenHuFuSortRule(Config config) {
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
    final RelTraitSet traitSet = sort.getTraitSet().replace(OpenHuFuRel.CONVENTION);
    final RelNode input = sort.getInput();
    return new OpenHuFuSort(
            relNode.getCluster(),
            traitSet,
            convert(input, input.getTraitSet().replace(OpenHuFuRel.CONVENTION)),
            sort.getCollation());
  }
}
