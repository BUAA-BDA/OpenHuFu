package com.hufudb.onedb.core.client;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.EnumerableDataSet;
import com.hufudb.onedb.core.implementor.UserSideImplementor;
import com.hufudb.onedb.core.rewriter.BasicRewriter;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.QueryPlanPool;
import com.hufudb.onedb.rewriter.Rewriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;

/**
 * client for all DB
 */
public class OneDBClient {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBClient.class);
  private final OneDBSchemaManager schemaManager;
  final ExecutorService threadPool;
  private final AtomicInteger queryId;
  private final Rewriter rewriter;

  public OneDBClient(OneDBSchemaManager schemaManager) {
    this.schemaManager = schemaManager;
    this.threadPool = Executors.newFixedThreadPool(OneDBConfig.CLIENT_THREAD_NUM);
    this.queryId = new AtomicInteger(0);
    this.rewriter = new BasicRewriter(this);
  }

  int getQueryId() {
    return queryId.getAndIncrement();
  }

  int getQueryId(int offset) {
    return queryId.getAndAdd(offset);
  }

  public long getTaskId() {
    return (((long) schemaManager.getUserId()) << 32) | (long) getQueryId();
  }

  public long getTaskId(int offset) {
    return (((long) schemaManager.getUserId()) << 32) | (long) getQueryId(offset);
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public boolean hasOwner(String endpoint) {
    return schemaManager.hasOwner(endpoint);
  }

  public OwnerClient getOwnerClient(String endpoint) {
    return schemaManager.getOwnerClient(endpoint);
  }

  public Set<String> getEndpoints() {
    return schemaManager.getEndpoints();
  }

  // add global table through zk
  public void addTable2Schema(String tableName, Table table) {
    schemaManager.addTable(tableName, table);
  }

  // drop global table
  public void dropTable(String tableName) {
    schemaManager.dropTable(tableName);
  }

  public OneDBTableSchema getTableSchema(String tableName) {
    return schemaManager.getTableSchema(tableName);
  }

  public boolean hasTable(String tableName) {
    return schemaManager.hasTable(tableName);
  }

  // for local table
  public void removeLocalTable(String globalTableName, String endpoint, String localTableName) {
    OneDBTableSchema table = getTableSchema(globalTableName);
    if (table == null) {
      LOG.error("Gloabl table {} not exists", globalTableName);
      return;
    }
    OwnerClient client = getOwnerClient(endpoint);
    if (client == null) {
      LOG.error("Endpoint {} not exists", endpoint);
    }
    table.dropLocalTable(client, localTableName);
  }

  public void dropLocalTable(String tableName, String endpoint) {
    schemaManager.dropLocalTable(tableName, endpoint);
  }

  public Schema getSchema(String tableName) {
    OneDBTableSchema table = getTableSchema(tableName);
    return table != null ? table.getSchema() : null;
  }

  public List<Pair<OwnerClient, String>> getTableClients(String tableName) {
    OneDBTableSchema table = getTableSchema(tableName);
    return table != null ? table.getTableList() : ImmutableList.of();
  }

  public DataSet executeQueryPlan(Plan plan) {
    plan = plan.rewrite(rewriter);
    return UserSideImplementor.getImplementor(plan, this).implement(plan);
  }

  /**
   * onedb query
   */
  public Enumerator<Row> oneDBQuery(long planId) {
    Plan plan = QueryPlanPool.getPlan(planId);
    // todo: support for choosing the appropritate rewriter
    return new EnumerableDataSet(executeQueryPlan(plan));
  }
}
