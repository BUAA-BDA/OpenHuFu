package com.hufudb.onedb.owner.postgresql;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.translator.OneDBTranslator;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PostgresqlService extends OwnerService {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlService.class);
  private final String catalog;
  private DatabaseMetaData metaData;
  private Connection connection;

  public PostgresqlService(
      String hostname,
      int port,
      String catalog,
      String url,
      String user,
      String passwd,
      List<POJOPublishedTableInfo> infos) {
    super(null, null, String.format("%s:%d", hostname, port), null);
    this.catalog = catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url, user, passwd);
      loadAllTableInfo();
      initPublishedTable(infos);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public PostgresqlService(
      String hostname, int port, String catalog, String url, String user, String passwd) {
    this(hostname, port, catalog, url, user, passwd, ImmutableList.of());
  }

  PostgresqlService(PostgresqlConfig config) {
    super(
        config.zkservers,
        config.zkroot,
        String.format(
            "%s:%d", config.hostname == null ? "localhost" : config.hostname, config.port),
        config.digest);
    this.catalog = config.catalog;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(config.url, config.user, config.passwd);
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
        if (header.getLevel(i).equals(Level.HIDDEN)) {
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

  String generateSQL(OneDBQueryProto query) {
    String originTableName = getOriginTableName(query.getTableName());
    Header tableHeader = getPublishedTableHeader(query.getTableName());
    LOG.info("{}: {}", originTableName, tableHeader);
    List<String> filters =
        OneDBTranslator.tranlateExps(
            tableHeader, OneDBExpression.fromProto(query.getWhereExpList()));
    List<String> selects =
        OneDBTranslator.tranlateExps(
            tableHeader, OneDBExpression.fromProto(query.getSelectExpList()));
    List<String> groups = query.getGroupList().stream()
        .map(ref -> tableHeader.getName(ref)).collect(Collectors.toList());
    // order by
    List<String> order = query.getOrderList();
    StringBuilder orderClause = new StringBuilder();
    if (!order.isEmpty()) {
      for (int i = 0; i < order.size(); i++) {
        String[] tmp = order.get(i).split(" ");
        orderClause.append(selects.get(Integer.parseInt(tmp[0]))).append(" ").append(tmp[1]);
        if (i != order.size() - 1) {
          orderClause.append(" , ");
        }
      }
    }
    if (query.getAggExpCount() > 0) {
      selects =
          OneDBTranslator.translateAgg(selects, OneDBExpression.fromProto(query.getAggExpList()));
    }
    // select clause
    StringBuilder sql =
        new StringBuilder(
            String.format("SELECT %s from %s", String.join(",", selects), originTableName));
    // where clause
    if (!filters.isEmpty()) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    if (!groups.isEmpty()) {
      sql.append(String.format(" group by %s", String.join(",", groups)));
    }
    if (orderClause.length() > 0) {
      sql.append(" ORDER BY ");
    }
    sql.append(orderClause);
    if (query.getFetch() != 0) {
      sql.append(" LIMIT ").append(query.getFetch() + query.getOffset());
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
