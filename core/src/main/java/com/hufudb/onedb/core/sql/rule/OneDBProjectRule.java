package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBProject;
import com.hufudb.onedb.core.sql.rel.OneDBRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class OneDBProjectRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalProject.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBProjectRule")
          .withRuleFactory(OneDBProjectRule::new);

  OneDBProjectRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalProject project = (LogicalProject) rel;
    final RelNode child =
        convert(project.getInput(), project.getInput().getTraitSet().replace(OneDBRel.CONVENTION));
    final RelTraitSet traitSet = child.getCluster().traitSet().replace(OneDBRel.CONVENTION);
    return new OneDBProject(
        child.getCluster(), traitSet, child, project.getProjects(), project.getRowType());
  }
}
