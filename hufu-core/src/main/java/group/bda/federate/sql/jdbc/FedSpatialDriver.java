package group.bda.federate.sql.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;

public class FedSpatialDriver extends Driver {
  final Function0<CalcitePrepare> prepareFactory;

  static {
    new FedSpatialDriver().register();
  }

  public FedSpatialDriver() {
    super();
    this.prepareFactory = createPrepareFactory();
  }

  protected Function0<CalcitePrepare> createPrepareFactory() {
    return FedSpatialPrepare.DEFAULT_FACTORY;
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:fedspatial:";
  }
}
