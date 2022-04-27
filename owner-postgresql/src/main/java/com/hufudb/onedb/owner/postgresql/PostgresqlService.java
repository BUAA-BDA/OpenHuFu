package com.hufudb.onedb.owner.postgresql;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.owner.OwnerService;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PostgresqlService extends OwnerService {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlService.class);
  private final String catalog;
  private DatabaseMetaData metaData;
  private Connection connection;
  private Statement statement;

  public PostgresqlService(
      String hostname,
      int port,
      String catalog,
      String url,
      String user,
      String passwd,
      List<POJOPublishedTableInfo> infos,
      ExecutorService threadPool) {
    super(null, null, String.format("%s:%d", hostname, port), null, threadPool);
    this.catalog = catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url, user, passwd);
      statement = connection.createStatement();
      loadAllTableInfo();
      initPublishedTable(infos);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public PostgresqlService(
      String hostname, int port, String catalog, String url, String user, String passwd, ExecutorService threadPool) {
    this(hostname, port, catalog, url, user, passwd, ImmutableList.of(), threadPool);
  }

  PostgresqlService(PostgresqlConfig config, ExecutorService threadPool) {
    super(
        config.zkservers,
        config.zkroot,
        String.format(
            "%s:%d", config.hostname == null ? "localhost" : config.hostname, config.port),
        config.digest,
        threadPool);
    this.catalog = config.catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      statement = connection.createStatement();
      loadAllTableInfo();
      initPublishedTable(config.tables);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void loadAllTableInfo() {
    try {
      metaData = connection.getMetaData();
      ResultSet rs = metaData.getTables(catalog, null, "%", new String[] {"TABLE"});
      while (rs.next()) {
        addLocalTableInfo(loadTableInfo(rs.getString("TABLE_NAME")));
      }
      rs.close();
    } catch (SQLException e) {
      LOG.error("Failed to load all tables: {}", e.getCause());
      e.printStackTrace();
    }
  }

  @Override
  protected TableInfo loadTableInfo(String tableName) {
    try {
      ResultSet rc = metaData.getColumns(catalog, null, tableName, null);
      TableInfo.Builder tableInfoBuilder = TableInfo.newBuilder();
      tableInfoBuilder.setTableName(tableName);
      while (rc.next()) {
        String columnName = rc.getString("COLUMN_NAME");
        tableInfoBuilder.add(
            columnName, PostgresqlTypeConverter.convert(rc.getString("TYPE_NAME")), Level.PUBLIC);
      }
      rc.close();
      return tableInfoBuilder.build();
    } catch (SQLException e) {
      LOG.error("Error when load tableinfo of {}: ", tableName, e.getMessage());
      return null;
    }
  }

  @Override
  protected Statement getStatement() {
      return statement;
  }

  @Override
  protected void beforeStop() {
    try {
      statement.close();
      connection.close();
    } catch (SQLException e) {
      LOG.error("Fail to close statement/connection: {}", e.getMessage());
    }
  }
}
