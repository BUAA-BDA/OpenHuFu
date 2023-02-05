package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class OpenHuFuToEnumerableConverterRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              RelNode.class,
              OpenHuFuRel.CONVENTION,
              EnumerableConvention.INSTANCE,
                  "OpenHuFuToEnumerableConverterRule")
          .withRuleFactory(OpenHuFuToEnumerableConverterRule::new);

  protected OpenHuFuToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new OpenHuFuToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
