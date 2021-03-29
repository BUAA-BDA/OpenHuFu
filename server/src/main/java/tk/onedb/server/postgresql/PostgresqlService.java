package tk.onedb.server.postgresql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.data.DataSet;
import tk.onedb.core.data.Header;
import tk.onedb.core.data.Level;
import tk.onedb.core.data.Row;
import tk.onedb.core.sql.translator.OneDBTranslator;
import tk.onedb.rpc.OneDBCommon.OneDBQueryProto;
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

  String generateSQL(OneDBQueryProto proto) {
    String tableName = proto.getTableName();
    Header tableHeader = getTableHeader(tableName);
    List<String> filters = OneDBTranslator.tranlateExps(tableHeader, proto.getWhereExpList());
    List<String> selects = OneDBTranslator.tranlateExps(tableHeader, proto.getSelectExpList());
    if (!proto.getAggExpList().isEmpty()) {
      selects = OneDBTranslator.translateAgg(selects, proto.getAggExpList());
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
  protected void oneDBQueryInternal(OneDBQueryProto query, DataSet dataSet) throws SQLException {
    String sql = generateSQL(query);
    if (sql.isEmpty()) {
      return;
    }
    executeSQL(sql, dataSet);
  }
}
