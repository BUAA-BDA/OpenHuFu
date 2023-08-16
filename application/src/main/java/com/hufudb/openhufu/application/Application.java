package com.hufudb.openhufu.application;

import com.google.gson.Gson;
import com.hufudb.openhufu.core.config.wyx_task.WXY_ConfigFile;
import com.hufudb.openhufu.core.config.wyx_task.WXY_InputDataItem;
import com.hufudb.openhufu.core.config.wyx_task.WXY_Party;
import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.utils.PojoColumnDesc;
import com.hufudb.openhufu.data.storage.ArrayDataSet;
import com.hufudb.openhufu.data.storage.ResultDataSet;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.owner.OwnerServer;
import com.hufudb.openhufu.user.OpenHuFuUser;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Map;

public class Application {
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  public static void main(String[] args) throws ClassNotFoundException, ParseException, IOException, SQLException, InterruptedException {
    Class.forName("org.postgresql.Driver");
    Options options = new Options();
    Option cmdConfig = new Option("c", "config", true, "owner config file path");
    cmdConfig.setRequired(true);
    options.addOption(cmdConfig);
    Option cmdConfig2 = new Option("t", "task", true, "task file path");
    cmdConfig2.setRequired(true);
    options.addOption(cmdConfig2);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    cmd = parser.parse(options, args);
    String ownerConfigPath = cmd.getOptionValue("config");
    String taskFilePath = cmd.getOptionValue("task");
    String jobId = System.getenv("jobID");
    String taskName = System.getenv("taskName");
    String task = getTask(jobId, taskName);
    WXY_ConfigFile wxy_configFile;
    if (!task.equals("")) {
      wxy_configFile = new Gson().fromJson(task, WXY_ConfigFile.class);
      LOG.info("GET TASK FROM REDIS");
    } else {
       Reader reader = Files.newBufferedReader(Paths.get(taskFilePath));
       wxy_configFile = new Gson().fromJson(reader, WXY_ConfigFile.class);
       LOG.info("GET TASK FROM CONFIG_FILE");
    }
    Thread owner = new Thread(new Runnable() {
      @Override
      public void run() {
        startOwner(ownerConfigPath, wxy_configFile);
      }
    });
    owner.start();

    Thread user = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          checkAndStartUser(wxy_configFile);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    user.start();


  }

  private static void startOwner(String ownerConfigPath, WXY_ConfigFile wxy_configFile) {
    try {
      OwnerServer server = OwnerServer.create(ownerConfigPath, wxy_configFile);
      server.start();
      server.blockUntilShutdown();
    } catch (Exception e) {
      LOG.error("Error when start owner server", e);
      System.exit(1);
    }
  }

  private static void checkAndStartUser(WXY_ConfigFile wxy_configFile) throws IOException, SQLException, InterruptedException {
    String domainID = System.getenv("orgDID");
    OpenHuFuUser user = new OpenHuFuUser();
    final int[] count = {0};
    for (WXY_InputDataItem dataItem: wxy_configFile.input.getData()) {
      if (dataItem.getDomainID().equals(domainID) && dataItem.getRole().equals("server")) {
        LOG.info("{} is server, exiting", domainID);
        return;
      }
    }
    WXY_UserConfig userConfig = wxy_configFile.generateUserConfig();

    for (WXY_Party party: wxy_configFile.parties) {
      Thread test = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!user.testServerConnection(party.getPartyID(), party.getIp(), party.getPort())) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          synchronized (count) {
            count[0]++;
          }
        }
      });
      test.start();
    }

    while (count[0] != wxy_configFile.parties.size()) {
      Thread.sleep(200);
    }

    ResultSet dataset = user.executeTask(userConfig);
    if (wxy_configFile.module.getModuleName().equals("KNN")) {
      ArrayDataSet resultDataSet = ArrayDataSet.materialize(new ResultDataSet(generateSchema(dataset), dataset));
      for (Row row: resultDataSet.getRows()) {
        LOG.info(row.toString());
      }
      for (String endpoint : wxy_configFile.getOutputEndpoints()) {
        user.saveResult(endpoint, resultDataSet);
      }
    }
    LOG.info("task finish");
  }

  private static Schema generateSchema(ResultSet dataset) throws SQLException {
    Schema.Builder schema = Schema.newBuilder();
    ResultSetMetaData resultSet = dataset.getMetaData();
    for (int i = 1; i <= resultSet.getColumnCount(); i++) {
      String columnName = resultSet.getColumnName(i);
      String columnType = resultSet.getColumnTypeName(i);
      PojoColumnDesc columnDesc = new PojoColumnDesc();
      columnDesc.columnId = i - 1;
      columnDesc.name = columnName;
      columnDesc.type = WXY_ConfigFile.convertType(columnType);
      columnDesc.modifier = ModifierWrapper.PUBLIC;
      LOG.info("ColumnID: {}, Column Name: {}, Data Type: {}, Modifier: {}",
              columnDesc.columnId, columnDesc.name, columnDesc.type, columnDesc.modifier);
      schema.add(columnDesc.toColumnDesc());
    }
    return schema.build();
  }
  public static String getTask(String jobId, String taskName) {
    String host = "redis-master";
    int port = 6379;
    int database = 0;
    String pwd = "Wlty*Ny1b!";
    String taskId = jobId + '_' + taskName;
    Jedis jedis = new Jedis(host, port);
    jedis.auth(pwd);
    jedis.select(database);
    String taskJson = jedis.get(taskId);
    LOG.info("TASK: {}", taskJson);
    return taskJson;
  }

}