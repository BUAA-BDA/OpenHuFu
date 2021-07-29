package group.bda.federate.sql.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rex.RexBuilder;

import group.bda.federate.sql.plan.FedSpatialDefaultMetadatProvider;

public class FedSpatialPrepare extends CalcitePrepareImpl {
  public static Function0<CalcitePrepare> DEFAULT_FACTORY = FedSpatialPrepare::new;

  @Override
  protected RelOptCluster createCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    cluster.setMetadataProvider(FedSpatialDefaultMetadatProvider.INSTANCE);
    return cluster;
  }
}
