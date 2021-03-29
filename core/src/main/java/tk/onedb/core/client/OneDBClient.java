package tk.onedb.core.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.config.OneDBConfig;
import tk.onedb.core.data.Header;
import tk.onedb.core.data.Row;
import tk.onedb.core.data.StreamBuffer;
import tk.onedb.core.sql.expression.OneDBQuery;
import tk.onedb.core.table.OneDBTableInfo;
import tk.onedb.core.utils.EmptyEnumerator;
import tk.onedb.rpc.OneDBCommon.DataSetProto;

/*
* client for all DB
*/
public class OneDBClient {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBClient.class);

  private final Map<String, DBClient> dbClientMap;
  private final Map<String, OneDBTableInfo> tableMap;
  private final ExecutorService executorService;

  public OneDBClient() {
    dbClientMap = new HashMap<>();
    tableMap = new HashMap<>();
    this.executorService = Executors.newFixedThreadPool(OneDBConfig.CLIENT_THREAD_NUM);
  }

  public Map<String, DBClient> getDBClientMap() {
    return dbClientMap;
  }

  public Map<String, OneDBTableInfo> getTableMap() {
    return tableMap;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public boolean addDB(String endpoint) {
    if (hasDB(endpoint)) {
      return false;
    }
    DBClient client = new DBClient(endpoint);
    for (Map.Entry<String, DBClient> entry : dbClientMap.entrySet()) {
      entry.getValue().addClient(endpoint);
      client.addClient(endpoint);
    }
    dbClientMap.put(endpoint, client);
    return true;
  }

  public boolean hasDB(String endpoint) {
    return dbClientMap.containsKey(endpoint);
  }

  public DBClient getDBClient(String endpoint) {
    return dbClientMap.get(endpoint);
  }

  // for global table
  public void addTable(String tableName, OneDBTableInfo table) {
    this.tableMap.put(tableName, table);
  }

  public void dropTable(String tableName) {
    tableMap.remove(tableName);
  }

  public OneDBTableInfo getTable(String tableName) {
    return tableMap.get(tableName);
  }

  public boolean hasTable(String tableName) {
    return tableMap.containsKey(tableName);
  }

  // for local table
  public void addLocalTable(String globalTableName, DBClient client, String localTableName) {
    OneDBTableInfo table = getTable(globalTableName);
    if (table != null) {
      table.addDB(client, localTableName);
    }
  }

  public Header getHeader(String tableName) {
    OneDBTableInfo table = getTable(tableName);
    return table != null ? table.getHeader() : null;
  }

  public Map<DBClient, String> getTableClients(String tableName) {
    OneDBTableInfo table = getTable(tableName);
    return table != null ? table.getTableMap() : null;
  }

  /*
  * onedb query
  */
  public Enumerator<Row> oneDBQuery(String tableName, OneDBQuery query) {
    Map<DBClient, String> tableClients = getTableClients(tableName);
    StreamBuffer<DataSetProto> streamProto = oneDBQuery(getHeader(tableName), query, tableClients);
    return new EmptyEnumerator<>();
  }

  private StreamBuffer<DataSetProto> oneDBQuery(Header header, OneDBQuery query, Map<DBClient, String> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Entry<DBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> {
        try {
          Iterator<DataSetProto> it = entry.getKey().oneDBQuery(query.toProto());
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = executorService.invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in fedSpatialPublicQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return iterator;
  }
}
