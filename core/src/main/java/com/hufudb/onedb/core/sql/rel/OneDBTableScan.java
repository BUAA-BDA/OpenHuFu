package com.hufudb.onedb.core.sql.rel;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.core.sql.rule.OneDBRules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataType;

public class OneDBTableScan extends TableScan implements OneDBRel {
  final OneDBTable oneDBTable;
  final RelDataType projectRowType;

  public OneDBTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, OneDBTable oneDBTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.oneDBTable = oneDBTable;
    this.projectRowType = projectRowType;
  }

  public Header getHeader() {
    return oneDBTable.getHeader();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.05);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(OneDBRules.TO_ENUMERABLE);
    for (RelOptRule rule : OneDBRules.RULES) {
      planner.addRule(rule);
    }
    planner.removeRule(JoinCommuteRule.Config.DEFAULT.toRule());
    planner.removeRule(JoinPushThroughJoinRule.LEFT);
    planner.removeRule(JoinPushThroughJoinRule.RIGHT);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.setTableName(oneDBTable.getTableName());
    implementor.setSchema(oneDBTable.getSchema());
    implementor.setSelectExps(OneDBReference.fromHeader(oneDBTable.getHeader()));
  }
}