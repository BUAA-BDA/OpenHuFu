package group.bda.federate.sql.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import group.bda.federate.sql.join.FedSpatialDistanceJoin;
import group.bda.federate.sql.join.FedSpatialJoin;
import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.join.FedSpatialKnnJoin;
import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.operator.FedSpatialToEnumerableConverter;
import group.bda.federate.sql.operator.FederateAggregate;
import group.bda.federate.sql.operator.FederateCalc;
import group.bda.federate.sql.operator.FederateFilter;
import group.bda.federate.sql.operator.FederateLimit;
import group.bda.federate.sql.operator.FederateProject;
import group.bda.federate.sql.operator.FederateSort;
import group.bda.federate.sql.operator.FederateTableScan;

public class FedSpatialRules {
  private FedSpatialRules() {
  }

  public static final FedSpatialFilterRule FILTER = FedSpatialFilterRule.Config.DEFAULT.toRule();

  public static final FedSpatialProjectRule PROJECT = FedSpatialProjectRule.DEFAULT_CONFIG
          .toRule(FedSpatialProjectRule.class);

  public static final FedSpatialAggregateRule AGGREGATE = FedSpatialAggregateRule.DEFAULT_CONFIG.toRule(FedSpatialAggregateRule.class);

  public static final FedSpatialCalcRule CALC = FedSpatialCalcRule.Config.DEFAULT.toRule();

  public static final FedSpatialLimitRule LIMIT = FedSpatialLimitRule.Config.DEFAULT.toRule();

  public static final FedSpatialJoinRule JOIN = FedSpatialJoinRule.DEFAUL_CONFIG.toRule(FedSpatialJoinRule.class);

  public static final FedSpatialToEnumerableConverterRule TO_ENUMERABLE = FedSpatialToEnumerableConverterRule.DEFAULT_CONFIG
          .toRule(FedSpatialToEnumerableConverterRule.class);

  public static final FedSpatialSortRule SORT = FedSpatialSortRule.Config.DEFAULT.toRule();

  public static final RelOptRule[] RULES = { CALC, AGGREGATE, FILTER, PROJECT, LIMIT, SORT, JOIN };

  public static class FedSpatialProjectRule extends ConverterRule {

    private static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalProject.class, Convention.NONE, FedSpatialRel.CONVENTION, "FedSpatialProjectRule")
            .withRuleFactory(FedSpatialProjectRule::new);

    FedSpatialProjectRule(Config config) {
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
      return new FederateProject(project.getCluster(), traitSet, convert(project.getInput(), out),
              project.getProjects(), project.getRowType());
    }
  }

  public static class FedSpatialFilterRule extends RelRule<FedSpatialFilterRule.Config> {

    protected FedSpatialFilterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      return disjunctions.size() == 1;
    }

    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY
              .withOperandSupplier(
                      b0 -> b0.operand(LogicalFilter.class).oneInput(b1 -> b1.operand(FederateTableScan.class).noInputs()))
              .as(Config.class);

      @Override
      default FedSpatialFilterRule toRule() {
        return new FedSpatialFilterRule(this);
      }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      FederateTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    RelNode convert(LogicalFilter filter, FederateTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(FedSpatialRel.CONVENTION);
      return new FederateFilter(filter.getCluster(), traitSet, convert(filter.getInput(), FedSpatialRel.CONVENTION),
              filter.getCondition(), scan.getHeader());
    }
  }

  public static class FedSpatialAggregateRule extends ConverterRule {
    private static final ConverterRule.Config DEFAULT_CONFIG = ConverterRule.Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE, FedSpatialRel.CONVENTION, "FedSpatialAggregateRule")
            .withRuleFactory(FedSpatialAggregateRule::new);

    protected FedSpatialAggregateRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return true;
    }

    @Override
    public RelNode convert(RelNode relNode) {
      final LogicalAggregate agg = (LogicalAggregate) relNode;
      final RelTraitSet traitSet = agg.getTraitSet().replace(out);
      return new FederateAggregate(agg.getCluster(), traitSet, convert(agg.getInput(), out), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
    }
  }

  public static class FedSpatialCalcRule extends RelRule<FedSpatialCalcRule.Config> {

    protected FedSpatialCalcRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final FedSpatialToEnumerableConverter converter = call.rel(1);
      final RelNode best = ((RelSubset) converter.getInput()).getBest();
      return !(best instanceof FedSpatialJoin);
    }

    public RelNode convert(RelNode relNode) {
      final EnumerableProject project = (EnumerableProject) relNode;
      final RelNode input = project.getInput();
      final RelTraitSet traitSet = project.getTraitSet().replace(FedSpatialRel.CONVENTION);
      final RexProgram program = RexProgram.create(input.getRowType(), project.getProjects(), null, project.getRowType(), project.getCluster().getRexBuilder());
      return new FederateCalc(project.getCluster(), traitSet, project.getHints(), convert(project.getInput(), FedSpatialRel.CONVENTION), program);
    }

    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY.withOperandSupplier(b0 -> b0.operand(EnumerableProject.class)
              .oneInput(b1 -> b1.operand(FedSpatialToEnumerableConverter.class).anyInputs())).as(Config.class);

      @Override
      default FedSpatialCalcRule toRule() {
        return new FedSpatialCalcRule(this);
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

  public static class FedSpatialLimitRule extends RelRule<FedSpatialLimitRule.Config> {

    protected FedSpatialLimitRule(Config config) {
      super(config);
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet = limit.getTraitSet().replace(FedSpatialRel.CONVENTION);
      return new FederateLimit(limit.getCluster(), traitSet, convert(limit.getInput(), FedSpatialRel.CONVENTION),
              limit.offset, limit.fetch);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final EnumerableLimit limit = call.rel(0);
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY.withOperandSupplier(b0 -> b0.operand(EnumerableLimit.class)
              .oneInput(b1 -> b1.operand(FedSpatialToEnumerableConverter.class).anyInputs())).as(Config.class);

      @Override
      default FedSpatialLimitRule toRule() {
        return new FedSpatialLimitRule(this);
      }
    }
  }

  public static class FedSpatialSortRule extends RelRule<FedSpatialSortRule.Config> {

    protected FedSpatialSortRule(Config config) {
      super(config);
    }

    public RelNode convert(EnumerableSort sort) {
      final RelTraitSet traitSet = sort.getTraitSet().replace(FedSpatialRel.CONVENTION).replace(sort.getCollation());
      return new FederateSort(sort.getCluster(), traitSet, convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final EnumerableSort sort = call.rel(0);
      final RelNode converted = convert(sort);
      if (converted != null) {
        call.transformTo(converted);
      }
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY.withOperandSupplier(b0 -> b0.operand(EnumerableSort.class)
              .oneInput(b1 -> b1.operand(FedSpatialToEnumerableConverter.class).anyInputs())).as(Config.class);

      @Override
      default FedSpatialSortRule toRule() {
        return new FedSpatialSortRule(this);
      }
    }
  }

  public static class FedSpatialJoinRule extends ConverterRule {

    public static final Config DEFAUL_CONFIG = Config.INSTANCE
    .withConversion(LogicalJoin.class, Convention.NONE, FedSpatialRel.CONVENTION, "FedSpatialJoinRule")
    .withRuleFactory(FedSpatialJoinRule::new);

    protected FedSpatialJoinRule(Config config) {
      super(config);
    }

    public boolean isKnnJoin(RexNode condition) {
      if (!(condition instanceof RexCall)) {
        return false;
      }
      RexCall call = (RexCall) condition;
      if (call.getKind().equals(SqlKind.OTHER_FUNCTION)) {
        SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
        return function.getName().equals("KNN");
      }
      return false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      LogicalJoin join = call.rel(0);
      RexNode condition = join.getCondition();
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
        return false;
      } else {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(disjunctions.get(0));
        if (conjunctions.size() != 1) {
          return false;
        }
        condition = conjunctions.get(0);
        return FedSpatialJoinInfo.support(condition);
      }
    }

    @Override
    public RelNode convert(RelNode rel) {
      LogicalJoin join = (LogicalJoin) rel;
      final RelTraitSet traitSet = join.getTraitSet().replace(out);
      List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() instanceof FedSpatialRel)) {
          input = convert(input, input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      final RelNode left = newInputs.get(0);
      final RelNode right = newInputs.get(1);
      final RexCall condition = (RexCall) join.getCondition();
      if (isKnnJoin(condition)) {
        return new FedSpatialKnnJoin(join.getCluster(), traitSet, left, right, join.getCondition(), join.getJoinType());
      } else {
        return new FedSpatialDistanceJoin(join.getCluster(), traitSet, left, right, join.getCondition(), join.getJoinType());
      }
    }
  }
}
