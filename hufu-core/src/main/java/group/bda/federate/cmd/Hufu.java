package group.bda.federate.cmd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import group.bda.federate.sql.schema.FederateSchema;
import group.bda.federate.sql.schema.FederateSchemaFactory;
import group.bda.federate.sql.table.FederateTableFactory;
import group.bda.federate.sql.table.TableMeta;

public class Hufu {
  private CalciteConnection calciteConnection;
  private FederateSchema schema;
  private Connection connection;

  private void setUp(List<String> endpoints, List<TableMeta> metas) throws SQLException, ClassNotFoundException {
    Class.forName("group.bda.federate.sql.jdbc.HufuJDBCDriver");
    Properties props = new Properties();
    props.setProperty("lex", "JAVA");
    props.setProperty("caseSensitive", "false");
    connection = DriverManager.getConnection("jdbc:fedspatial:", props);
    calciteConnection = connection.unwrap(CalciteConnection.class);
    calciteConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    schema = FederateSchemaFactory.INSTANCE.create(rootSchema, endpoints);
    for (TableMeta meta : metas) {
      rootSchema.add(meta.tableName, FederateTableFactory.INSTANCE.create(schema, meta));
    }
    rootSchema.add("fedSpatial", schema);
  }

  public ResultSet executeQuery(String sql) {
    try {
      Statement statement = connection.createStatement();
      return statement.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private Hufu(List<String> endpoints, List<TableMeta> meta) {
    try {
      setUp(endpoints, meta);
    } catch (Exception e) {
      System.out.println("error in setup");
      e.printStackTrace();
    }
  }

  public static FedSpatialBuilder newBuilder() {
    return new FedSpatialBuilder();
  }

  public static Hufu fromConfig(List<String> endpoints, List<TableMeta> tableMetas) {
    return new Hufu(endpoints, tableMetas);
  }

  public static class FedSpatialBuilder {
    private List<String> endpoints;
    private List<TableMeta> tableMetas;

    private FedSpatialBuilder() {
      this.endpoints = new ArrayList<>();
      this.tableMetas = new ArrayList<>();
    }

    public FedSpatialBuilder addEndpoint(String endpoint) {
      endpoints.add(endpoint);
      return this;
    }

    public FedSpatialBuilder addEndpoints(List<String> endpoints) {
      this.endpoints.addAll(endpoints);
      return this;
    }

    public FedSpatialBuilder addTable(TableMeta tableMeta) {
      for (TableMeta.FedMeta fedMeta : tableMeta.feds) {
        if (!endpoints.contains(fedMeta.endpoint)) {
          System.out.printf("Add table %s failed : endpoint[%s] not exist\n", tableMeta.tableName, fedMeta.endpoint);
          return this;
        }
      }
      tableMetas.add(tableMeta);
      return this;
    }

    public Hufu build() {
      return new Hufu(endpoints, tableMetas);
    }
  }
}
