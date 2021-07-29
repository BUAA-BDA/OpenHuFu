package group.bda.federate.sql.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import group.bda.federate.data.Header;
import group.bda.federate.sql.expression.FedSpatialExpressions;
import group.bda.federate.sql.rules.FedSpatialRules;
import group.bda.federate.sql.table.FederateTable;

public class FederateTableScan extends TableScan implements FedSpatialRel {
  final FederateTable fedTable;
  final RelDataType projectRowType;

  public FederateTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, FederateTable fedTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.fedTable = fedTable;
    this.projectRowType = projectRowType;
  }

  public Header getHeader() {
    return fedTable.getHeader();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(FedSpatialRules.TO_ENUMERABLE);
    for (RelOptRule rule : FedSpatialRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.setFederateTable(fedTable);
    implementor.setTable(table);
    implementor.setSelectExps(FedSpatialExpressions.create(fedTable.getHeader()));
  }
}
