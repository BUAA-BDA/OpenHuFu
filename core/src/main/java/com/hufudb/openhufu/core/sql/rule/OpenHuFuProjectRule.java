package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuProject;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class OpenHuFuProjectRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalProject.class, Convention.NONE, OpenHuFuRel.CONVENTION, "OpenHuFuProjectRule")
          .withRuleFactory(OpenHuFuProjectRule::new);

  OpenHuFuProjectRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalProject project = (LogicalProject) rel;
    final RelNode child =
        convert(project.getInput(), project.getInput().getTraitSet().replace(OpenHuFuRel.CONVENTION));
    final RelTraitSet traitSet = child.getCluster().traitSet().replace(OpenHuFuRel.CONVENTION);
    return new OpenHuFuProject(
        child.getCluster(), traitSet, child, project.getProjects(), project.getRowType());
  }
}
