package group.bda.federate.driver;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.data.Row;
import group.bda.federate.data.StreamDataSet;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateService.Query;
import group.bda.federate.driver.config.ServerConfig;
import group.bda.federate.driver.table.ServerTableInfo;
import group.bda.federate.driver.utils.MysqlTypeConverter;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.driver.utils.AggCache;
import group.bda.federate.driver.utils.DistanceDataSet;
import group.bda.federate.driver.utils.MysqlConfig;
import group.bda.federate.driver.utils.MysqlIRTranslator;
import io.grpc.ServerBuilder;

public class MysqlServer extends FederateDBServer {
  private static class MysqlService extends FederateDBService {
    private static final Logger LOG = LogManager.getLogger(MysqlService.class);
     private Connection connection;
     private Statement st;
     private DatabaseMetaData metaData;

    MysqlService(MysqlConfig config) {
      try {
        init(config);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void init(MysqlConfig config) throws Exception {
      Class.forName("com.mysql.cj.jdbc.Driver");
      connection = DriverManager.getConnection(config.url, config.user, config.passwd);
      st = connection.createStatement();
      metaData = connection.getMetaData();
      String catalog = connection.getCatalog();
      ResultSet rs = metaData.getTables(catalog, null, "%", new String[]{"TABLE"});
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        ServerConfig.Table table = config.getTable(tableName);
        if (table == null) {
          continue;
        }
        ResultSet rc = metaData.getColumns(catalog, null, tableName, null);
        ServerTableInfo.IteratorBuilder tableInfoBuilder = ServerTableInfo.newBuilder();
        tableInfoBuilder.setTableName(tableName);
        while (rc.next()) {
          String columnName = rc.getString("COLUMN_NAME");
          String typeName = rc.getString("TYPE_NAME");
          Level level = table.getLevel(columnName);
          if (level.equals(Level.HIDE)) continue;
          LOG.info("col: {}, type: {}", columnName, typeName);
          tableInfoBuilder.add(columnName, MysqlTypeConverter.convert(typeName), level);
        }
        try {
          ServerTableInfo tableInfo = tableInfoBuilder.build();
          LOG.info("add {}", tableInfo.toString());
          tableInfoMap.put(tableName, tableInfo);
        } catch (Exception e) {
          LOG.warn("fail to add table {}", tableName);
          e.printStackTrace();
        }
      }
    }

    private void fillDataSet(ResultSet rs, StreamDataSet dataSet) throws SQLException {
      final Header header = dataSet.getHeader();
      final int columnSize = header.size();
      while (rs.next()) {
        Row.RowBuilder builder = Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          if (header.getLevel(i).equals(Level.HIDE)) {
            if (header.getType(i) == FederateFieldType.POINT) {
              builder.set(i, new group.bda.federate.sql.type.Point(0, 0));
            } else {
              LOG.error("the type should not be hidden");
              throw new RuntimeException("the type should not be hidden");
            }
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

    private void fillAggregateDataSet(ResultSet rs, String aggUuid, StreamDataSet dataSet) throws SQLException {
      final Header header = dataSet.getHeader();
      final int columnSize = header.size();
      while (rs.next()) {
        Row.RowBuilder builder = Row.newBuilder(columnSize);
        Row.RowBuilder rawBuilder = Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          boolean hide = false;
          if (header.getLevel(i).equals(Level.HIDE)) {
            hide = true;
          }
          switch (header.getType(i)) {
            case SHORT:
              builder.set(i, hide ? (short) 0 : rs.getShort(i + 1));
              rawBuilder.set(i, rs.getShort(i + 1));
              break;
            case INT:
              builder.set(i, hide ? 0 : rs.getInt(i + 1));
              rawBuilder.set(i, rs.getInt(i + 1));
              break;
            case LONG:
              builder.set(i, hide ? (long) 0 : rs.getLong(i + 1));
              rawBuilder.set(i, rs.getLong(i + 1));
              break;
            case FLOAT:
              builder.set(i, hide ? (float) 0 : rs.getFloat(i + 1));
              rawBuilder.set(i, rs.getFloat(i + 1));
              break;
            case DOUBLE:
              builder.set(i, hide ? (double) 0 : rs.getDouble(i + 1));
              rawBuilder.set(i, rs.getDouble(i + 1));
              break;
            default:
              LOG.error("not support aggregate type");
              break;
          }
        }
        Row row = builder.build();
        buffer.set(aggUuid, new AggCache(rawBuilder.build(), header));
        dataSet.addRow(row);
      }
    }

    private DistanceDataSet fillDistanceDataSet(Header header, ResultSet rs) throws SQLException {
      final int columnSize = header.size();
      final int actualColumnSize = rs.getMetaData().getColumnCount();
      DataSet dataSet = DataSet.newDataSet(header);
      List<Double> distances = new ArrayList<>();
      while (rs.next()) {
        DataSet.DataRowBuilder builder = dataSet.newRow();
        if (actualColumnSize == columnSize + 1) {
          distances.add(rs.getDouble(columnSize + 1));
          // LOG.debug("the distance is {}", rs.getDouble(columnSize + 1));
        }
        for (int i = 0; i < columnSize; ++i) {
          if (header.getLevel(i).equals(Level.HIDE)) {
            if (header.getType(i) == FederateFieldType.DOUBLE) {
              distances.add(rs.getDouble(i + 1));
              // LOG.debug("the distance is {}", rs.getDouble(i + 1));
              builder.setUnsafe(i, (double) 0);
            } else if (header.getType(i) == FederateFieldType.POINT) {
              builder.setUnsafe(i, new group.bda.federate.sql.type.Point(0, 0));
            } else {
              LOG.error("the type should not be hidden");
              throw new RuntimeException("the type should not be hidden");
            }
            continue;
          }
          switch (header.getType(i)) {
            case BYTE:
              builder.setUnsafe(i, rs.getByte(i + 1));
              break;
            case SHORT:
              builder.setUnsafe(i, rs.getShort(i + 1));
              break;
            case INT:
              builder.setUnsafe(i, rs.getInt(i + 1));
              break;
            case LONG:
              builder.setUnsafe(i, rs.getLong(i + 1));
              break;
            case FLOAT:
              builder.setUnsafe(i, rs.getFloat(i + 1));
              break;
            case DOUBLE:
              builder.setUnsafe(i, rs.getFloat(i + 1));
              break;
            case STRING:
              builder.setUnsafe(i, rs.getString(i + 1));
              break;
            case BOOLEAN:
              builder.setUnsafe(i, rs.getBoolean(i + 1));
              break;
            case DATE:
              // Divide by 86400000L to prune time field
              Long date = rs.getDate(i + 1).getTime() / 86400000L;
              builder.setUnsafe(i, date);
              break;
            case TIME:
              Long time = rs.getTime(i + 1).getTime();
              builder.setUnsafe(i, time);
              break;
            case TIMESTAMP:
              Long timeStamp = rs.getTimestamp(i + 1).getTime();
              builder.setUnsafe(i, timeStamp);
              break;
            default:
              break;
          }
        }
        builder.build();
      }
      return new DistanceDataSet(dataSet, distances);
    }

    private void executeSQL(String sql, StreamDataSet dataSet, String aggUuid) throws SQLException {
      ResultSet rs = st.executeQuery(sql);
      if (dataSet.getHeader().isPrivacyAgg()) {
        fillAggregateDataSet(rs, aggUuid, dataSet);
      } else {
        fillDataSet(rs, dataSet);
      }
      LOG.info("Execute {} returned {} rows", sql, dataSet.getRowCount());
    }

    @Override
    public DistanceDataSet calKnn(Query query)
            throws SQLException {
      String sql = generateSQL(query);
      if (sql.isEmpty()) {
        return null;
      }
      ResultSet rs = st.executeQuery(sql);
      DistanceDataSet dataSet = fillDistanceDataSet(Header.fromProto(query.getHeader()), rs);
      LOG.info("Execute {} returned {} rows", sql, dataSet.size());
      return dataSet;
    }

    @Override
    public void fedSpatialQueryInternal(Query request, StreamDataSet streamDataSet) throws SQLException {
      String sql = generateSQL(request);
      String aggUuid = "";
      if (request.hasAggUuid()) {
        aggUuid = request.getAggUuid();
      }
      if (sql.isEmpty()) {
        return;
      }
      executeSQL(sql, streamDataSet, aggUuid);
    }

    private String generateSQL(Query request) {
      String tableName = request.getTableName();
      Header tableHeader = getTableHeader(tableName);
      int fetch = Integer.MAX_VALUE;
      List<String> projectStr = new ArrayList<>();
      for (Expression e : request.getProjectExpList()) {
        projectStr.add(new MysqlIRTranslator(e, tableHeader).translate());
      }
      List<String> order = request.getOrderList();
      // order by clause
      StringBuilder orderClause = new StringBuilder();
      if (!order.isEmpty()) {
        for (int i = 0; i < order.size(); i++) {
          String[] tmp = order.get(i).split(" ");
          orderClause.append(projectStr.get(Integer.parseInt(tmp[0]))).append(" ").append(tmp[1]);
          if (i != order.size() - 1) {
            orderClause.append(" , ");
          }
        }
      }
      List<String> filter = new ArrayList<>();
      for (Expression exp : request.getFilterExpList()) {
        MysqlIRTranslator translator = new MysqlIRTranslator(exp, tableHeader);
        filter.add(translator.translate());
        IR kNN = translator.getKNN();
        if (kNN != null) {
          fetch = translator.getKOfkNN(kNN);
          String column = translator.getColumnOfkNN(kNN);
          group.bda.federate.sql.type.Point point = translator.getPointOfkNN(kNN);
          if (orderClause.length() != 0) {
            orderClause.append(" , ");
          }
          orderClause.append(String.format(" Distance(%s, ST_GeomFromText('POINT(%s %s)')) ASC ", column, String.valueOf(point.getX()), String.valueOf(point.getY())));
          projectStr.add(String.format("Distance(%s, ST_GeomFromText('POINT(%s %s)'))", column, String.valueOf(point.getX()), String.valueOf(point.getY())));
        }
      }
      fetch = request.getFetch() == Integer.MAX_VALUE ? fetch : request.getFetch();
      // select clause
      StringBuilder sql = new StringBuilder(String.format("SELECT %s from %s", String.join(",", projectStr), tableName));
      // where clause
      if (request.getFilterExpList().size() != 0) {
        sql.append(String.format(" WHERE %s", String.join(" AND ", filter)));
      }
      //order clause
      if (orderClause.length() > 0) {
        sql.append(" ORDER BY ");
      }
      sql.append(orderClause);
      // limit clause
      if (fetch != Integer.MAX_VALUE) {
        sql.append(" LIMIT ").append(fetch);
      }
      LOG.info(sql.toString());
      return sql.toString();
    }
  }

  public MysqlServer(MysqlConfig config) throws IOException {
    super(ServerBuilder.forPort(config.port), config.port, new MysqlService(config));
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addRequiredOption("c", "config", true, "Mysql config path");
    CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      MysqlConfig pConfig = gson.fromJson(reader, MysqlConfig.class);
      MysqlServer server = new MysqlServer(pConfig);
      server.start();
      server.blockUntilShutdown();
    } catch (ParseException | IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}