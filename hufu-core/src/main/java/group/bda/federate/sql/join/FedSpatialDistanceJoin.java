package group.bda.federate.sql.join;

import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.expression.FedSpatialExpressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import group.bda.federate.data.Header;

public class FedSpatialDistanceJoin extends FedSpatialJoin {

  public FedSpatialDistanceJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
      RelNode right, RexNode condition, JoinRelType joinType) {
    super(cluster, traitSet, left, right, condition, joinType);
  }

  @Override
  public void implement(FedSpatialRel.Implementor implementor) {
    implementor.visitChild(0, getLeft());
    implementor.packTableQueryParams();
    implementor.visitChild(0, getRight());
    implementor.packTableQueryParams();
    // todo: judge security level of both and pick public one as left
    Header.IteratorBuilder builder = Header.newBuilder();
    FedSpatialRel.TableQueryParams left = implementor.getQueryParams(0);
    FedSpatialRel.TableQueryParams right = implementor.getQueryParams(1);
    buildHeader(builder, left);
    buildHeader(builder, right);
    Header header = builder.build();
    implementor.setHeader(header);
    FedSpatialJoinInfo joinInfo = FedSpatialJoinInfo.generateJoinInfo(getCondition(), left.getProjectExps(), right.getProjectExps());
    implementor.setJoinInfo(joinInfo);
    implementor.setSelectExps(FedSpatialExpressions.create(header));
  }
}
