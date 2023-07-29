package com.hufudb.openhufu.core.config.wyx_task;

import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.core.table.LocalTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WXY_ConfigFile {
  private static final int THREAD_NUM = 8;
  public static final Logger LOG = LoggerFactory.getLogger(WXY_ConfigFile.class);

  public String version;
  public String jobID;
  public String taskName;
  public String status;
  public String updateTime;
  public String createTime;

  public WXY_Module module;

  public WXY_Input input;

  public List<WXY_Party> parties;

  public WXY_UserConfig generateUserConfig() {
    WXY_UserConfig userConfig = new WXY_UserConfig();

    //step 1 generate endpoints
    List<String> endpoints = new ArrayList<>();
    for (WXY_Party party: parties) {
      endpoints.add(party.getPartyID());
    }
    userConfig.endpoints = endpoints;

    //step 2 generate global table configs
    List<GlobalTableConfig> globalTableConfigs = new ArrayList<>();
    GlobalTableConfig globalTableConfig = new GlobalTableConfig();
    globalTableConfig.tableName = "global_" + input.getData().get(0).getTable();
    List<LocalTableConfig> localTableConfigs = new ArrayList<>();
    for (WXY_DataItem dataItem: input.getData()) {
      LocalTableConfig localTableConfig = new LocalTableConfig();
      localTableConfig.localName = dataItem.getTable();
      localTableConfig.endpoint = dataItem.getDomainID();
      localTableConfigs.add(localTableConfig);
    }
    globalTableConfig.localTables = localTableConfigs;
    globalTableConfigs.add(globalTableConfig);
    userConfig.globalTableConfigs = globalTableConfigs;

    //step 3 generate sql
    userConfig.sql = generateSQL(globalTableConfig.tableName,
            input.getData().get(0).getField());

    return userConfig;
  }

  private String generateSQL(String globalTable, String field) {
    String sql = "select ";
    switch (module.getModuleName()) {
      case "RANGEQUERY":
        sql += "* from " + globalTable
                + " where DWithin(" + module.getPoint() + ", "
                + field + ", " + module.getRange() + ")";
        break;
      case "RANGECOUNT":
        sql += "count(*) from " + globalTable
                + " where DWithin(" + module.getPoint() + ", "
                + field + ", " + module.getRange() + ")";
        break;
      case "KNN":
        sql += "* from " + globalTable
                + " where KNN(" + module.getPoint() + ", "
                + field + ", " + module.getK() + ")";
        break;
      default:
        LOG.error("not support {}", module.getModuleName());
    }
    LOG.info("generate sql: {}", sql);
    return sql;
  }

}
