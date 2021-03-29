package tk.onedb.core.sql.rule;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import tk.onedb.core.sql.rel.OneDBRel;
import tk.onedb.core.sql.rel.OneDBToEnumerableConverter;

public class OneDBToEnumerableConverterRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG = Config.INSTANCE.withConversion(RelNode.class, OneDBRel.CONVENTION,
      EnumerableConvention.INSTANCE, "FedSpatialToEnumerableConverterRule")
      .withRuleFactory(OneDBToEnumerableConverterRule::new);

  protected OneDBToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new OneDBToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
