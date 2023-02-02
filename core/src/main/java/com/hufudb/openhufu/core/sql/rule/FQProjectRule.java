package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQProject;
import com.hufudb.openhufu.core.sql.rel.FQRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class FQProjectRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalProject.class, Convention.NONE, FQRel.CONVENTION, "OneDBProjectRule")
          .withRuleFactory(FQProjectRule::new);

  FQProjectRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalProject project = (LogicalProject) rel;
    final RelNode child =
        convert(project.getInput(), project.getInput().getTraitSet().replace(FQRel.CONVENTION));
    final RelTraitSet traitSet = child.getCluster().traitSet().replace(FQRel.CONVENTION);
    return new FQProject(
        child.getCluster(), traitSet, child, project.getProjects(), project.getRowType());
  }
}
