package group.bda.federate.sql.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.type.FederateFieldType;


public abstract class FedSpatialJoin extends Join implements FedSpatialRel {
  protected FedSpatialJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
      RelNode right, RexNode condition, JoinRelType joinType) {
    super(cluster, traitSet, ImmutableList.of(), left, right, condition, ImmutableSet.of(), joinType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new FedSpatialDistanceJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
  }

  protected void buildHeader(Header.IteratorBuilder builder, TableQueryParams params) {
    for (FedSpatialExpression e : params.getProjectExps()) {
      FederateFieldType type = e.getType();
      Level level = e.getLevel();
      builder.add("", type, level);
    }
  }
}
