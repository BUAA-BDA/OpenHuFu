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
import com.hufudb.onedb.user.utils.OneDBLine;
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

  public OneDB() {
    try {
      Class.forName("com.hufudb.onedb.user.jdbc.OneDBDriver");
      Properties props = new Properties();
      props.setProperty("lex", "JAVA");
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

  public static void main(String[] args) {
    final Options options = new Options();
    final Option model = new Option("m", "model", true, "model of fed");
    model.setRequired(true);
    options.addOption(model);
    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      Class.forName("com.hufudb.onedb.user.jdbc.OneDBDriver");
      cmd = parser.parse(options, args);
      final String m = cmd.getOptionValue("model", "model.json");
      List<String> dbargs = new ArrayList<>();
      dbargs.add("-u");
      dbargs.add("jdbc:onedb:model=" + m + ";lex=JAVA;caseSensitive=false;");
      dbargs.add("-n");
      dbargs.add("admin");
      dbargs.add("-p");
      dbargs.add("admin");
      OneDBLine.start(dbargs.toArray(new String[6]), null, true);
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
}
