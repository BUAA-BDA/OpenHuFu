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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDB {

  protected final static Logger LOG = LoggerFactory.getLogger(OneDB.class);
  private CalciteConnection calciteConnection;
  private OneDBSchemaManager schema;
  private Connection connection;

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
      LOG.error("Create onedb error", e);
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
      LOG.error("Setup cli error", e);
    }
  }

  public ResultSet executeQuery(String sql) {
    try {
      Statement statement = connection.createStatement();
      return statement.executeQuery(sql);
    } catch (SQLException e) {
      LOG.error("Execute query error", e);
    }
    return null;
  }

  public DataSet executeQuery(Plan plan) {
    return schema.query(plan);
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.error("Close connection error", e);
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
}
