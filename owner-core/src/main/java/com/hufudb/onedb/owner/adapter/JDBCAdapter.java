package com.hufudb.onedb.owner.adapter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.translator.OneDBTranslator;
import com.hufudb.onedb.owner.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base adapter for datasource with jdbc support
 */
public abstract class JDBCAdapter implements DataSourceAdapter {
  protected final static  Logger LOG = LoggerFactory.getLogger(JDBCAdapter.class);

  protected String catalog;
  protected Connection connection;
  protected Statement statement;
  protected final DataSourceTypeConverter converter;
  protected final SchemaManager schemaManager;

  protected JDBCAdapter(String catalog, Connection connection, Statement statement, DataSourceTypeConverter converter) {
    this.catalog = catalog;
    this.connection = connection;
    this.statement = statement;
    this.converter = converter;
    this.schemaManager = new SchemaManager();
    loadAllTableInfo();
  }

  public void loadAllTableInfo() {
    try {
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet rs = meta.getTables(catalog, null, "%", new String[] {"TABLE"});
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        schemaManager.addLocalTable(getTableInfo(tableName, meta));
      }
      rs.close();
    } catch (Exception e) {
      LOG.error("Fail to load all table info: {}", e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void query(OneDBContext queryContext, DataSet dataSet) {
    String sql = generateSQL(queryContext);
    if (!sql.isEmpty()) {
      executeSQL(sql, dataSet);
    }
  }

  @Override
  public void beforeStop() {
    try {
      statement.close();
      connection.close();
    } catch (Exception e) {
      LOG.error("Fail to close statement/connection: {}", e.getMessage());
    }
  }

  @Override
  public SchemaManager getSchemaManager() {
    return schemaManager;
  }

  protected TableInfo getTableInfo(String tableName, DatabaseMetaData meta) {
    try {
      ResultSet rc = meta.getColumns(catalog, null, tableName, null);
      TableInfo.Builder tableInfoBuilder = TableInfo.newBuilder();
      tableInfoBuilder.setTableName(tableName);
      while (rc.next()) {
        String columnName = rc.getString("COLUMN_NAME");
        tableInfoBuilder.add(columnName, converter.convert(rc.getString("TYPE_NAME")),
            Level.PUBLIC);
      }
      rc.close();
      return tableInfoBuilder.build();
    } catch (Exception e) {
      LOG.error("Error when load tableinfo of {}: ", tableName, e.getMessage());
      return null;
    }
  }

  protected String generateSQL(OneDBContext query) {
    String originTableName = schemaManager.getLocalTableName(query.getTableName());
    Header tableHeader = schemaManager.getPublishedTableHeader(query.getTableName());
    LOG.info("{}: {}", originTableName, tableHeader);
    final List<String> filters = OneDBTranslator.translateExps(tableHeader, query.getWhereExps());
    final List<String> selects = OneDBTranslator.translateExps(tableHeader,
        query.getSelectExps());
    final List<String> groups =
        query.getGroups().stream().map(ref -> selects.get(ref)).collect(Collectors.toList());
    // order by
    List<String> order = OneDBTranslator.translateOrders(selects, query.getOrders());
    StringBuilder sql = new StringBuilder();
    // select from clause
    if (!query.getAggExps().isEmpty()) {
      final List<String> aggs =
          OneDBTranslator.translateAgg(selects, query.getAggExps());
      sql.append(String.format("SELECT %s from %s", String.join(",", aggs), originTableName));
    } else {
      sql.append(String.format("SELECT %s from %s", String.join(",", selects), originTableName));
    }
    // where clause
    if (!filters.isEmpty()) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    if (!groups.isEmpty()) {
      sql.append(String.format(" group by %s", String.join(",", groups)));
    }
    if (!order.isEmpty()) {
      sql.append(String.format(" order by %s", String.join(",", order)));
    }
    if (query.getFetch() != 0) {
      sql.append(" LIMIT ").append(query.getFetch() + query.getOffset());
    }
    LOG.info(sql.toString());
    return sql.toString();
  }

  protected void fillDataSet(ResultSet rs, DataSet dataSet) throws SQLException {
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
            builder.set(i, rs.getObject(i + 1));
            break;
        }
      }
      dataSet.addRow(builder.build());
    }
  }

  protected void executeSQL(String sql, DataSet dataSet) {
    ResultSet rs = null;
    try {
      rs = statement.executeQuery(sql);
      fillDataSet(rs, dataSet);
      LOG.info("Execute {} returned {} rows", sql, dataSet.getRowCount());
      rs.close();
    } catch (SQLException e) {
      LOG.error("Fail to execute SQL [{}]: {}", sql, e.getMessage());
    }
  }
}
