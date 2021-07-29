package group.bda.federate.sql.rules;

import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.operator.FedSpatialToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class FedSpatialToEnumerableConverterRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG = Config.INSTANCE.withConversion(RelNode.class, FedSpatialRel.CONVENTION,
      EnumerableConvention.INSTANCE, "FedSpatialToEnumerableConverterRule")
      .withRuleFactory(FedSpatialToEnumerableConverterRule::new);

  protected FedSpatialToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new FedSpatialToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
