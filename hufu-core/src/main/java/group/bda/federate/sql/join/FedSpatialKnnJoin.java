package group.bda.federate.sql.join;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import group.bda.federate.data.Header;

public class FedSpatialKnnJoin extends FedSpatialJoin {

  public FedSpatialKnnJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
    RelNode right, RexNode condition, JoinRelType joinType) {
    super(cluster, traitSet, left, right, condition, joinType);
  }

  @Override
  public void implement(Implementor implementor) {
  implementor.visitChild(0, getLeft());
  implementor.packTableQueryParams();
  implementor.visitChild(0, getRight());
  implementor.packTableQueryParams();
  // todo: judge security level of both and pick public one as left
  Header.IteratorBuilder builder = Header.newBuilder();
  TableQueryParams left = implementor.getQueryParams(0);
  TableQueryParams right = implementor.getQueryParams(1);
  buildHeader(builder, left);
  buildHeader(builder, right);
  Header header = builder.build();
  implementor.setHeader(header);
  getCondition();
  FedSpatialJoinInfo joinInfo = FedSpatialJoinInfo.generateJoinInfo(condition, left.getProjectExps(), right.getProjectExps());
  implementor.setJoinInfo(joinInfo);
  }
}
