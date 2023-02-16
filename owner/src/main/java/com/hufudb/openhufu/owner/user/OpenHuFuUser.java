package com.hufudb.openhufu.owner.user;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.core.client.OwnerClient;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuTable;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaFactory;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.core.table.OpenHuFuTableSchema;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.plan.Plan;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * @author yang.song
 * @date 2/15/23 5:06 PM
 */
public class OpenHuFuUser {
  private OpenHuFuSchemaManager manager;

  public OpenHuFuUser() {
    try {
      SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
      manager =
          (OpenHuFuSchemaManager)
              OpenHuFuSchemaFactory.INSTANCE.create(
                  rootSchema, "openhufu", new HashMap<String, Object>());
      rootSchema.add("openhufu", manager);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public DataSet executeQuery(Plan plan) {
    return manager.query(plan);
  }

  // for DB
  public Set<String> getEndpoints() {
    return manager.getEndpoints();
  }

  public boolean addOwner(String endpoint) {
    manager.addOwner(endpoint, null);
    return true;
  }

  public boolean addOwner(String endpoint, String certPath) {
    manager.addOwner(endpoint, certPath);
    return true;
  }

  public void removeOwner(String endpoint) {
    manager.removeOwner(endpoint);
  }

  public List<TableSchema> getOwnerTableSchema(String endpoint) {
    OwnerClient client = manager.getOwnerClient(endpoint);
    if (client  == null) {
      return ImmutableList.of();
    } else {
      return client.getAllLocalTable();
    }
  }

  // for table
  public List<OpenHuFuTableSchema> getAllOpenHuFuTableSchema() {
    return manager.getAllOpenHuFuTableSchema();
  }

  public OpenHuFuTableSchema getOpenHuFuTableSchema(String tableName) {
    return manager.getTableSchema(tableName);
  }

  public boolean createOpenHuFuTable(GlobalTableConfig meta) {
    return OpenHuFuTable.create(manager, meta) != null;
  }

  public void dropOpenHuFuTable(String tableName) {
    manager.dropTable(tableName);
  }

  public boolean addLocalTable(String OpenHuFuTableName, String endpoint, String localTableName) {
    return manager.addLocalTable(OpenHuFuTableName, endpoint, localTableName);
  }

  public void dropLocalTable(String OpenHuFuTableName, String endpoint, String localTableName) {
    manager.dropLocalTable(OpenHuFuTableName, endpoint, localTableName);
  }

}
