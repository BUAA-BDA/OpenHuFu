package tk.onedb.server.postgresql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import tk.onedb.OneDBService.Query;
import tk.onedb.core.data.Level;
import tk.onedb.rpc.OneDBCommon.DataSetProto;
import tk.onedb.server.DBService;
import tk.onedb.server.data.ServerConfig;
import tk.onedb.server.data.TableInfo;

public class PostgresqlService extends DBService {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlService.class);
  private DatabaseMetaData metaData;
  private Connection connection;
  private final String catalog;

  PostgresqlService(PostgresqlConfig config) {
    this.catalog = config.catalog;
    try {
      init(config);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void init(PostgresqlConfig config) throws ClassNotFoundException, SQLException {
    Class.forName("org.postgresql.Driver");
    connection = DriverManager.getConnection(config.url, config.user, config.passwd);
    metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables(catalog, null, "%", new String[]{"TABLE"});
    while (rs.next()) {
      String tableName = rs.getString("TABLE_NAME");
      ServerConfig.Table table = config.getTable(tableName);
      if (table == null) {
        continue;
      }
      try {
        addTable(table);
      } catch (SQLException e) {
        LOG.warn("Failed to add table {} : {}", table.name, e.getCause());
      }
    }
  }

  void addTable(ServerConfig.Table table) throws SQLException {
    ResultSet rc = metaData.getColumns(catalog, null, table.name, null);
    TableInfo.Builder tableInfoBuilder = TableInfo.newBuilder();
    tableInfoBuilder.setTableName(table.name);
    while (rc.next()) {
      String columnName = rc.getString("COLUMN_NAME");
      Level level = table.getLevel(columnName);
      if (level.equals(Level.HIDE)) continue;
      tableInfoBuilder.add(columnName, PostgresqlTypeConverter.convert(rc.getString("TYPE_NAME")), level);
    }
    try {
      TableInfo tableInfo = tableInfoBuilder.build();
      LOG.info("add {}", tableInfo.toString());
      tableInfoMap.put(table.name, tableInfo);
    } catch (Exception e) {
      LOG.warn("fail to add table {}", table.name);
      e.printStackTrace();
    }
  }

  @Override
  public void oneDBQuery(Query request, StreamObserver<DataSetProto> responseObserver) {
    super.oneDBQuery(request, responseObserver);
  }
}
