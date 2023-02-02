package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQRel;
import com.hufudb.openhufu.core.sql.rel.FQToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class FQToEnumerableConverterRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              RelNode.class,
              FQRel.CONVENTION,
              EnumerableConvention.INSTANCE,
              "OneDBToEnumerableConverterRule")
          .withRuleFactory(FQToEnumerableConverterRule::new);

  protected FQToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new FQToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
