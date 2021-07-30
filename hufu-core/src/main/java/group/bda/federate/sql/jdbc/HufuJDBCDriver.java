package group.bda.federate.sql.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;

public class HufuJDBCDriver extends Driver {
  final Function0<CalcitePrepare> prepareFactory;

  static {
    new HufuJDBCDriver().register();
  }

  public HufuJDBCDriver() {
    super();
    this.prepareFactory = createPrepareFactory();
  }

  protected Function0<CalcitePrepare> createPrepareFactory() {
    return FedSpatialPrepare.DEFAULT_FACTORY;
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:hufu:";
  }
}
