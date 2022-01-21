package com.hufudb.onedb.server.postgresql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.hufudb.onedb.core.data.AliasTableInfo;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.sql.expression.OneDBQuery;
import com.hufudb.onedb.core.sql.translator.OneDBTranslator;
import com.hufudb.onedb.server.DBService;

@Service
public class PostgresqlService extends DBService {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlService.class);
  private DatabaseMetaData metaData;
  private Connection connection;
  private final String catalog;

  public PostgresqlService(String hostname, int port, String catalog, String url, String user, String passwd) {
    super(null, null, String.format("%s:%d", hostname, port), null);
    this.catalog = catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url, user, passwd);
      loadAllTableInfo();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  PostgresqlService(PostgresqlConfig config) {
    super(config.zkservers, config.zkroot,
        String.format("%s:%d", config.hostname == null ? "localhost" : config.hostname, config.port), config.digest);
    this.catalog = config.catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      loadAllTableInfo();
      initVirtualTable(config.tables);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void loadAllTableInfo() {
    try {
      metaData = connection.getMetaData();
      ResultSet rs = metaData.getTables(catalog, null, "%", new String[] { "TABLE" });
      while (rs.next()) {
        addLocalTableInfo(loadTableInfo(rs.getString("TABLE_NAME")));
      }
    } catch (SQLException e) {
      LOG.error("Failed to load all tables: {}", e.getCause());
      e.printStackTrace();
    }
  }

  public void initVirtualTable(List<AliasTableInfo> infos) {
    for (AliasTableInfo info : infos) {
      addVirtualTable(info);
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
        tableInfoBuilder.add(columnName, PostgresqlTypeConverter.convert(rc.getString("TYPE_NAME")), Level.PUBLIC);
      }
      return tableInfoBuilder.build();
    } catch (SQLException e) {
      LOG.error("Error when load tableinfo of {}: ", tableName, e.getMessage());
      return null;
    }
  }

  private void fillDataSet(ResultSet rs, DataSet dataSet) throws SQLException {
    final Header header = dataSet.getHeader();
    final int columnSize = header.size();
    while (rs.next()) {
      Row.RowBuilder builder = Row.newBuilder(columnSize);
      for (int i = 0; i < columnSize; ++i) {
        if (header.getLevel(i).equals(Level.HIDE)) {
          continue;
        }
        switch (header.getType(i)) {
        case BYTE:
          builder.set(i, rs.getByte(i + 1));
          break;
        case SHORT:
          builder.set(i, rs.getShort(i + 1));
          break;
        case INT:
          builder.set(i, rs.getInt(i + 1));
          break;
        case LONG:
          builder.set(i, rs.getLong(i + 1));
          break;
        case FLOAT:
          builder.set(i, rs.getFloat(i + 1));
          break;
        case DOUBLE:
          builder.set(i, rs.getDouble(i + 1));
          break;
        case STRING:
          builder.set(i, rs.getString(i + 1));
          break;
        case BOOLEAN:
          builder.set(i, rs.getBoolean(i + 1));
          break;
        case DATE:
          // Divide by 86400000L to prune time field
          Long date = rs.getDate(i + 1).getTime() / 86400000L;
          builder.set(i, date);
          break;
        case TIME:
          Long time = rs.getTime(i + 1).getTime();
          builder.set(i, time);
          break;
        case TIMESTAMP:
          Long timeStamp = rs.getTimestamp(i + 1).getTime();
          builder.set(i, timeStamp);
          break;
        default:
          break;
        }
      }
      dataSet.addRow(builder.build());
    }
  }

  String generateSQL(OneDBQuery query) {
    String tableName = query.tableName;
    Header tableHeader = getVirtualTableHeader(tableName);
    List<String> filters = OneDBTranslator.tranlateExps(tableHeader, query.filterExps);
    List<String> selects = OneDBTranslator.tranlateExps(tableHeader, query.selectExps);
    if (!query.aggExps.isEmpty()) {
      selects = OneDBTranslator.translateAgg(selects, query.aggExps);
    }
    // select clause
    StringBuilder sql = new StringBuilder(String.format("SELECT %s from %s", String.join(",", selects), tableName));
    // where clause
    if (filters.size() != 0) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    LOG.info(sql.toString());
    return sql.toString();
  }

  void executeSQL(String sql, DataSet dataSet) throws SQLException {
    Statement st = connection.createStatement();
    ResultSet rs = st.executeQuery(sql);
    fillDataSet(rs, dataSet);
    LOG.info("Execute {} returned {} rows", sql, dataSet.getRowCount());
  }

  @Override
  protected void oneDBQueryInternal(OneDBQuery query, DataSet dataSet) throws SQLException {
    String sql = generateSQL(query);
    if (sql.isEmpty()) {
      return;
    }
    executeSQL(sql, dataSet);
  }
}
