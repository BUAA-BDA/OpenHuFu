package tk.onedb.core.sql.rule;

import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;

import org.immutables.value.Value;

import tk.onedb.core.sql.rel.OneDBCalc;
import tk.onedb.core.sql.rel.OneDBRel;
import tk.onedb.core.sql.rel.OneDBToEnumerableConverter;

public class OneDBCalcRule extends RelRule<OneDBCalcRule.OneDBCalcRuleConfig> {
  protected OneDBCalcRule(OneDBCalcRuleConfig config) {
    super(config);
  }

  public RelNode convert(RelNode relNode) {
    final EnumerableProject project = (EnumerableProject) relNode;
    final RelNode input = project.getInput();
    final RelTraitSet traitSet = project.getTraitSet().replace(OneDBRel.CONVENTION);
    final RexProgram program = RexProgram.create(input.getRowType(), project.getProjects(), null, project.getRowType(), project.getCluster().getRexBuilder());
    return new OneDBCalc(project.getCluster(), traitSet, project.getHints(), convert(project.getInput(), OneDBRel.CONVENTION), program);
  }

  @Value.Immutable
  public interface OneDBCalcRuleConfig extends RelRule.Config {
    OneDBCalcRuleConfig DEFAULT = ImmutableOneDBCalcRuleConfig.builder()
        .operandSupplier(b0 -> b0.operand(EnumerableProject.class)
            .oneInput(b1 -> b1.operand(OneDBToEnumerableConverter.class).anyInputs())).build();

    @Override
    default OneDBCalcRule toRule() {
      return new OneDBCalcRule(this);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final EnumerableProject project = call.rel(0);
    final RelNode converted = convert(project);
    if (converted != null) {
      call.transformTo(converted);
    }
  }
}
