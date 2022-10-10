package com.hufudb.onedb.user;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaFactory;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.user.utils.ModelGenerator;
import com.hufudb.onedb.user.utils.OneDBLine;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OneDB {
  private CalciteConnection calciteConnection;
  private OneDBSchemaManager schema;
  private Connection connection;

  private Connection DBConn;

  private static String getSystemTimeZone() {
    TimeZone timeZone = TimeZone.getDefault();
    long hours = TimeUnit.MILLISECONDS.toHours(timeZone.getRawOffset());
    return hours >= 0 ? String.format("gmt+%d", hours) : String.format("gmt%d", hours);
  }

  public OneDB() {
    try {
      Class.forName("com.hufudb.onedb.user.jdbc.OneDBDriver");
      Properties props = new Properties();
      props.setProperty("lex", "JAVA");
      props.setProperty("timeZone", getSystemTimeZone());
      props.setProperty("caseSensitive", "false");
      connection = DriverManager.getConnection("jdbc:onedb:", props);
      calciteConnection = connection.unwrap(CalciteConnection.class);
      calciteConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      schema =
          (OneDBSchemaManager)
              OneDBSchemaFactory.INSTANCE.create(
                  rootSchema, "onedb", new HashMap<String, Object>());
      rootSchema.add("onedb", schema);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void setupCLI(String configPath) throws IOException {
    List<String> dbargs = new ArrayList<>();
    dbargs.add("-u");
    dbargs.add(String.format("jdbc:onedb:model=%s;lex=JAVA;caseSensitive=false;timeZone='%s'", ModelGenerator.loadUserConfig(configPath), getSystemTimeZone()));
    dbargs.add("-n");
    dbargs.add("admin");
    dbargs.add("-p");
    dbargs.add("admin");
    OneDBLine.start(dbargs.toArray(new String[6]), null, true);
  }

  public static void main(String[] args) {
    final Options options = new Options();
    final Option config = new Option("c", "config", true, "user config file path");
    config.setRequired(true);
    options.addOption(config);
    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      Class.forName("com.hufudb.onedb.user.jdbc.OneDBDriver");
      cmd = parser.parse(options, args);
      final String configPath = cmd.getOptionValue("config", "config/user.json");
      setupCLI(configPath);
    } catch (Exception e) {
      e.printStackTrace();
    }
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

  public ResultSet executeQuery(String sql, long id) {
    String status;
    ResultSet ret = null;
    long start = System.currentTimeMillis();
    try {
      // get start Time; Status = In Process; store startTime into sqlRecord database(update status)
      Statement statement = connection.createStatement();

      String startSQL = "UPDATE sqlRecord set startTime = ?, status = ? where id = ?";
      PreparedStatement startPs = DBConn.prepareStatement(startSQL);
      status = "In Progress";
      startPs.setString(2, status);
      startPs.setLong(3, id);
      start = System.currentTimeMillis();
      Timestamp startTimestamp = new Timestamp(start);
      startPs.setTimestamp(1, startTimestamp);
      startPs.executeUpdate();

      ret = statement.executeQuery(sql);
      // get end Time; Status = Succeed
      status = "Succeed";

    } catch (SQLException e) {
      // get end Time; Status = Failed;
      e.printStackTrace();
      status = "Failed";
    }

    try {
      // execTime = endTime - startTime; store status execTime into sqlRecord database(update status)
      String endSQL = "UPDATE sqlRecord set status = ?, execTime = ? where id = ?";
      PreparedStatement endPs = DBConn.prepareStatement(endSQL);
      endPs.setString(1, status);
      endPs.setLong(3, id);
      long execTime = System.currentTimeMillis() - start;
      endPs.setLong(2, execTime);

      endPs.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }

  public DataSet executeQuery(Plan plan) {
    return schema.query(plan);
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // for DB
  public Set<String> getEndpoints() {
    return schema.getEndpoints();
  }

  public boolean addOwner(String endpoint) {
    schema.addOwner(endpoint, null);
    return true;
  }

  public boolean addOwner(String endpoint, String certPath) {
    schema.addOwner(endpoint, certPath);
    return true;
  }

  public void removeOwner(String endpoint) {
    schema.removeOwner(endpoint);
  }

  public List<TableSchema> getOwnerTableSchema(String endpoint) {
    OwnerClient client = schema.getOwnerClient(endpoint);
    if (client  == null) {
      return ImmutableList.of();
    } else {
      return client.getAllLocalTable();
    }
  }

  // for table
  public List<OneDBTableSchema> getAllOneDBTableSchema() {
    return schema.getAllOneDBTableSchema();
  }

  public OneDBTableSchema getOneDBTableSchema(String tableName) {
    return schema.getTableSchema(tableName);
  }

  public boolean createOneDBTable(GlobalTableConfig meta) {
    return OneDBTable.create(schema, meta) != null;
  }

  public void dropOneDBTable(String tableName) {
    schema.dropTable(tableName);
  }

  public boolean addLocalTable(String oneDBTableName, String endpoint, String localTableName) {
    return schema.addLocalTable(oneDBTableName, endpoint, localTableName);
  }

  public void dropLocalTable(String oneDBTableName, String endpoint, String localTableName) {
    schema.dropLocalTable(oneDBTableName, endpoint, localTableName);
  }

  public void setDBConn(Connection DBConn) {
    this.DBConn = DBConn;
  }
}
