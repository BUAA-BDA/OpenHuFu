package com.hufudb.onedb;

import com.hufudb.onedb.client.utils.OneDBLine;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaFactory;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.table.TableMeta;
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
  private OneDBSchema schema;
  private Connection connection;

  public OneDB() {
    try {
      Class.forName("com.hufudb.onedb.client.jdbc.OneDBDriver");
      Properties props = new Properties();
      props.setProperty("lex", "JAVA");
      props.setProperty("caseSensitive", "false");
      connection = DriverManager.getConnection("jdbc:onedb:", props);
      calciteConnection = connection.unwrap(CalciteConnection.class);
      calciteConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      schema =
          (OneDBSchema)
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
      Class.forName("com.hufudb.onedb.client.jdbc.OneDBDriver");
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
    return schema.addOwner(endpoint) != null;
  }

  public void removeOwner(String endpoint) {
    schema.removeOnwer(endpoint);
  }

  public List<TableInfo> getDBTableInfo(String endpoint) {
    return schema.getDBClient(endpoint).getAllLocalTable();
  }

  // for table
  public List<OneDBTableInfo> getAllOneDBTableInfo() {
    return schema.getAllOneDBTableInfo();
  }

  public OneDBTableInfo getOneDBTableInfo(String tableName) {
    return schema.getOneDBTableInfo(tableName);
  }

  public boolean createOneDBTable(TableMeta meta) {
    return OneDBTable.create(schema, meta) != null;
  }

  public void dropOneDBTable(String tableName) {
    schema.dropTable(tableName);
  }
}
