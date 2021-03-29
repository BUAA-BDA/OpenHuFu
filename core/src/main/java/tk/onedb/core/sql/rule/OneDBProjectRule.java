package tk.onedb.core.sql.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import tk.onedb.core.sql.rel.OneDBProject;
import tk.onedb.core.sql.rel.OneDBRel;

public class OneDBProjectRule extends ConverterRule {
  static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalProject.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBProjectRule")
      .withRuleFactory(OneDBProjectRule::new);

  OneDBProjectRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    for (RexNode e : project.getProjects()) {
      if (!(e instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalProject project = (LogicalProject) rel;
    final RelTraitSet traitSet = project.getTraitSet().replace(out);
    return new OneDBProject(project.getCluster(), traitSet, convert(project.getInput(), out), project.getProjects(),
        project.getRowType());
  }
}
