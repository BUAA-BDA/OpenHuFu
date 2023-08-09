package com.hufudb.openhufu.application;

import com.google.gson.Gson;
import com.hufudb.openhufu.core.config.wyx_task.WXY_ConfigFile;
import com.hufudb.openhufu.core.config.wyx_task.WXY_DataItem;
import com.hufudb.openhufu.core.config.wyx_task.WXY_Party;
import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.owner.OwnerServer;
import com.hufudb.openhufu.user.OpenHuFuUser;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

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

    Thread owner = new Thread(new Runnable() {
      @Override
      public void run() {
        startOwner(ownerConfigPath, taskFilePath);
      }
    });
    owner.start();

    Thread user = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          checkAndStartUser(taskFilePath);
        } catch (Exception e) {
          LOG.info("Error when start user client");
        }
      }
    });
    user.start();


  }
  private static void startOwner(String ownerConfigPath, String taskFilePath) {
    try {
      OwnerServer server = OwnerServer.create(ownerConfigPath, taskFilePath);
      server.start();
      server.blockUntilShutdown();
    } catch (Exception e) {
      LOG.error("Error when start owner server", e);
      System.exit(1);
    }
  }

  private static void checkAndStartUser(String taskFilePath) throws IOException, SQLException, InterruptedException {
    String domainID = System.getenv("DOMAIN_ID");
    String jobId = System.getenv("jobID");
    String taskName = System.getenv("taskName");
    OpenHuFuUser user = new OpenHuFuUser();
    String task = user.getTask(jobId, taskName);
    // Reader reader = Files.newBufferedReader(Paths.get(taskFilePath));
    // WXY_ConfigFile configFile = new Gson().fromJson(reader, WXY_ConfigFile.class);
    WXY_ConfigFile configFile = new Gson().fromJson(task, WXY_ConfigFile.class);
    final int[] count = {0};
    for (WXY_DataItem dataItem: configFile.input.getData()) {
      if (dataItem.getDomainID().equals(domainID) && dataItem.getRole().equals("server")) {
        LOG.info("{} is server, exiting", domainID);
        return;
      }
    }
    WXY_UserConfig userConfig = configFile.generateUserConfig();

    for (WXY_Party party: configFile.parties) {
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

    while (count[0] != configFile.parties.size()) {
      Thread.sleep(200);
    }

    ResultSet dataset = user.executeTask(userConfig);
    while (dataset.next()) {
      for (int i = 1; i <= dataset.getMetaData().getColumnCount(); i++) {
        System.out.print(dataset.getString(i) + "|");
      }
      System.out.println();
    }
  }
}