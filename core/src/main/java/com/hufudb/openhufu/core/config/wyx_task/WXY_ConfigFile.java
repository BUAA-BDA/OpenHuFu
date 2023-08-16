package com.hufudb.openhufu.core.config.wyx_task;

import com.hufudb.openhufu.core.config.wyx_task.user.WXY_UserConfig;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.core.table.LocalTableConfig;
import com.hufudb.openhufu.data.schema.utils.PojoColumnDesc;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  public WXY_Output output;
  public List<WXY_Party> parties;

  public WXY_UserConfig generateUserConfig() {
    WXY_UserConfig userConfig = new WXY_UserConfig();

    //step 1 generate endpoints
    HashMap<String, String> domainID2endpoint = new HashMap<>();
    List<String> endpoints = new ArrayList<>();
    for (WXY_Party party: parties) {
      endpoints.add(party.getEndpoint());
      domainID2endpoint.put(party.getPartyID(), party.getEndpoint());
    }
    userConfig.endpoints = endpoints;

    //step 2 generate global table configs
    List<GlobalTableConfig> globalTableConfigs = new ArrayList<>();
    GlobalTableConfig globalTableConfig = new GlobalTableConfig();
    globalTableConfig.tableName = "global_" + input.getData().get(0).getTable();
    List<LocalTableConfig> localTableConfigs = new ArrayList<>();
    for (WXY_InputDataItem dataItem: input.getData()) {
      LocalTableConfig localTableConfig = new LocalTableConfig();
      localTableConfig.localName = dataItem.getTable();
      localTableConfig.endpoint = domainID2endpoint.get(dataItem.getDomainID());
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

  public List<PojoPublishedTableSchema> getLocalSchemas(String domainId, String jdbcUrl, String username, String password) throws SQLException {
    List<PojoPublishedTableSchema> tableSchemas = new ArrayList<>();
    String publishName = input.getData().get(0).getTable();
    for (WXY_InputDataItem dataItem: input.getData()) {
      if (dataItem.getDomainID().equals(domainId)) {
        PojoPublishedTableSchema schema = new PojoPublishedTableSchema();
        schema.setActualName(dataItem.getTable());
        schema.setPublishedName(publishName);

        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        DatabaseMetaData metaData = connection.getMetaData();
        String tableName = dataItem.getTable();
        ResultSet resultSet = metaData.getColumns(null, null, tableName, null);

        List<PojoColumnDesc> columnDescs = new ArrayList<>();
        int columnCount = 0;
        while (resultSet.next()) {
          String columnName = resultSet.getString("COLUMN_NAME");
          String columnType = resultSet.getString("TYPE_NAME");
          PojoColumnDesc columnDesc = new PojoColumnDesc();
          columnDesc.columnId = columnCount;
          columnDesc.name = columnName;
          columnDesc.type = convertType(columnType);
          columnDesc.modifier = ModifierWrapper.PUBLIC;
          if (columnName.equalsIgnoreCase(dataItem.getField())) {
            columnDesc.modifier = ModifierWrapper.PROTECTED;
          }
          LOG.info("ColumnID: {}, Column Name: {}, Data Type: {}, Modifier: {}",
                  columnDesc.columnId, columnDesc.name, columnDesc.type, columnDesc.modifier);
          columnDescs.add(columnDesc);
          columnCount++;
        }
        schema.setPublishedColumns(columnDescs);
        tableSchemas.add(schema);
      }
    }
    return tableSchemas;
  }

  public HashMap<String, String> getOutputMap() {
    HashMap<String, String> domainID2endpoint = new HashMap<>();
    for (WXY_Party party: parties) {
      domainID2endpoint.put(party.getPartyID(), party.getEndpoint());
    }
    HashMap<String, String> endpoint2name = new HashMap<>();
    for (WXY_OutputDataItem dataItem: output.getData()) {
      endpoint2name.put(domainID2endpoint.get(dataItem.getDomainID()), dataItem.getDataName());
    }
    return endpoint2name;
  }

  public static ColumnTypeWrapper convertType(String columnType) {
    switch (columnType.toLowerCase()) {
      case "character varying":
      case "varchar":
      case "character":
      case "char":
      case "text":
        return ColumnTypeWrapper.STRING;
      case "smallserial":
      case "serial":
      case "serial2":
      case "serial4":
      case "smallint":
      case "integer":
      case "int":
      case "int2":
      case "int4":
        return ColumnTypeWrapper.INT;
      case "bigint":
      case "int8":
      case "bigserial":
      case "serial8":
        return ColumnTypeWrapper.LONG;
      case "real":
      case "float4":
        return ColumnTypeWrapper.FLOAT;
      case "decimal":
      case "double precision":
      case "numeric":
      case "float8":
      case "money":
        return ColumnTypeWrapper.DOUBLE;
      case "bit":
      case "boolean":
      case "bool":
        return ColumnTypeWrapper.BOOLEAN;
      case "time":
        return ColumnTypeWrapper.TIME;
      case "timestamp":
        return ColumnTypeWrapper.TIMESTAMP;
      case "geometry":
        return ColumnTypeWrapper.GEOMETRY;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + columnType);
    }
  }
}
