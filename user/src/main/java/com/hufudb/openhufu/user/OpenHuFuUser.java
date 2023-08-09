package com.hufudb.openhufu.user;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.hufudb.openhufu.core.client.OwnerClient;
import com.hufudb.openhufu.core.config.wyx_task.WXY_ConfigFile;
import com.hufudb.openhufu.core.config.wyx_task.WXY_DataItem;
import com.hufudb.openhufu.core.config.wyx_task.WXY_Party;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuTable;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaFactory;
import com.hufudb.openhufu.core.table.OpenHuFuTableSchema;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.proto.OpenHuFuService;
import com.hufudb.openhufu.proto.ServiceGrpc;
import com.hufudb.openhufu.user.utils.ModelGenerator;
import com.hufudb.openhufu.user.utils.OpenHuFuLine;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenHuFuUser {

  protected final static Logger LOG = LoggerFactory.getLogger(OpenHuFuUser.class);
  private CalciteConnection calciteConnection;
  private OpenHuFuSchemaManager schema;
  private Connection connection;

  private static String getSystemTimeZone() {
    TimeZone timeZone = TimeZone.getDefault();
    long hours = TimeUnit.MILLISECONDS.toHours(timeZone.getRawOffset());
    return hours >= 0 ? String.format("gmt+%d", hours) : String.format("gmt%d", hours);
  }

  public OpenHuFuUser() {
    try {
      Class.forName("com.hufudb.openhufu.user.jdbc.OpenHuFuDriver");
      Properties props = new Properties();
      props.setProperty("lex", "JAVA");
      props.setProperty("timeZone", getSystemTimeZone());
      props.setProperty("caseSensitive", "false");
      connection = DriverManager.getConnection("jdbc:openhufu:", props);
      calciteConnection = connection.unwrap(CalciteConnection.class);
      calciteConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      schema =
          (OpenHuFuSchemaManager)
              OpenHuFuSchemaFactory.INSTANCE.create(
                  rootSchema, "openhufu", new HashMap<String, Object>());
      rootSchema.add("openhufu", schema);
    } catch (Exception e) {
      LOG.error("Create openhufu error", e);
    }
  }

  public static void setupCLI(String configPath) throws IOException {
    List<String> dbargs = new ArrayList<>();
    dbargs.add("-u");
    dbargs.add(String.format("jdbc:openhufu:model=%s;lex=JAVA;caseSensitive=false;timeZone='%s'", ModelGenerator.loadUserConfig(configPath), getSystemTimeZone()));
    dbargs.add("-n");
    dbargs.add("admin");
    dbargs.add("-p");
    dbargs.add("admin");
    OpenHuFuLine.start(dbargs.toArray(new String[6]), null, true);
  }

  public static void main(String[] args) throws SQLException, ParseException, IOException {
    String domainID = System.getenv("DOMAIN_ID");
    final Options options = new Options();
    final Option config = new Option("c", "config", true, "user config file path");
    config.setRequired(true);
    options.addOption(config);
    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    cmd = parser.parse(options, args);
    final String configPath = cmd.getOptionValue("config", "config/user.json");
    OpenHuFuUser user = new OpenHuFuUser();
    Reader reader = Files.newBufferedReader(Paths.get(configPath));
    WXY_ConfigFile configFile = new Gson().fromJson(reader, WXY_ConfigFile.class);
    for (WXY_DataItem dataItem: configFile.input.getData()) {
      if (dataItem.getDomainID().equals(domainID) && dataItem.getRole().equals("server")) {
        LOG.info("{} is server, exiting", domainID);
        System.exit(0);
      }
    }
    WXY_UserConfig userConfig = configFile.generateUserConfig();
    ResultSet dataset = user.executeTask(userConfig);
    while (dataset.next()) {
      for (int i = 1; i <= dataset.getMetaData().getColumnCount(); i++) {
        System.out.print(dataset.getString(i) + "|");
      }
      System.out.println();
    }
  }

  public boolean testServerConnection(String domainID, String ip, int port) {
    ManagedChannel managedChannel = ManagedChannelBuilder
            .forAddress(ip, port)
            .usePlaintext()
            .build();
    try {
      ServiceGrpc.newBlockingStub(managedChannel).getOwnerInfo(OpenHuFuService.GeneralRequest.newBuilder().build());
      managedChannel.shutdown();
      LOG.info("server {} ({}:{}) has started.", domainID, ip, port);
      return true;
    } catch (StatusRuntimeException e) {
      managedChannel.shutdown();
      LOG.info("server {} ({}:{}) has not started yet.", domainID, ip, port);
      return false;
    }
  }

  public ResultSet executeTask(WXY_UserConfig userConfig) throws SQLException {
    for (String endpoint: userConfig.endpoints) {
      addOwner(endpoint);
    }
    for (GlobalTableConfig config: userConfig.globalTableConfigs) {
      createOpenHuFuTable(config);
    }
    LOG.info("Init finish");
    Statement statement = createStatement();
    ResultSet resultSet = statement.executeQuery(userConfig.sql);
//    statement.close();
    return resultSet;
  }

  public Statement createStatement() throws SQLException {
    return connection.createStatement();
  }

  public DataSet executeQuery(Plan plan) {
    return schema.query(plan);
  }

  public void close() {
    try {
      calciteConnection.close();
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
  public List<OpenHuFuTableSchema> getAllOpenHuFuTableSchema() {
    return schema.getAllOpenHuFuTableSchema();
  }

  public OpenHuFuTableSchema getOpenHuFuTableSchema(String tableName) {
    return schema.getTableSchema(tableName);
  }

  public boolean createOpenHuFuTable(GlobalTableConfig meta) {
    return OpenHuFuTable.create(schema, meta) != null;
  }

  public void dropOpenHuFuTable(String tableName) {
    schema.dropTable(tableName);
  }

  public boolean addLocalTable(String openHuFuTableName, String endpoint, String localTableName) {
    return schema.addLocalTable(openHuFuTableName, endpoint, localTableName);
  }

  public void dropLocalTable(String openHuFuTableName, String endpoint, String localTableName) {
    schema.dropLocalTable(openHuFuTableName, endpoint, localTableName);
  }
}
