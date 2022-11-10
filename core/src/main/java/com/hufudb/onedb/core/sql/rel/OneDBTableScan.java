package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.sql.rule.OneDBRules;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.LeafPlan;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableLimitRule;
import org.apache.calcite.adapter.enumerable.EnumerableProjectToCalcRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataType;

public class OneDBTableScan extends TableScan implements OneDBRel {
  final OneDBTable oneDBTable;
  final RelDataType projectRowType;

  public OneDBTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      OneDBTable oneDBTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.oneDBTable = oneDBTable;
    this.projectRowType = projectRowType;
  }

  public Schema getSchema() {
    return oneDBTable.getSchema();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.05);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this;
  }

  @Override
  public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(OneDBRules.TO_ENUMERABLE);
    for (RelOptRule rule : OneDBRules.RULES) {
      planner.addRule(rule);
    }
    planner.removeRule(JoinCommuteRule.Config.DEFAULT.toRule());
    planner.removeRule(JoinPushThroughJoinRule.LEFT);
    planner.removeRule(JoinPushThroughJoinRule.RIGHT);
    planner.removeRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
    planner.removeRule(EnumerableProjectToCalcRule.Config.DEFAULT.toRule());
    planner.removeRule(EnumerableLimitRule.Config.DEFAULT.toRule());
  }

  /**
   * Select from all the cols in this OneDBTable's schema
   * @param implementor
   */
  @Override
  public void implement(Implementor implementor) {
    LeafPlan plan = new LeafPlan();
    implementor.setSchemaManager(oneDBTable.getRootSchema());
    plan.setTableName(oneDBTable.getTableName());
    plan.setSelectExps(ExpressionFactory.createInputRef(oneDBTable.getSchema()));
    implementor.setCurrentPlan(plan);
  }
}
