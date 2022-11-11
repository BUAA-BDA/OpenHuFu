package com.hufudb.onedb.core.client;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.EnumerableDataSet;
import com.hufudb.onedb.core.implementor.UserSideImplementor;
import com.hufudb.onedb.core.rewriter.BasicRewriter;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.QueryPlanPool;
import com.hufudb.onedb.rewriter.Rewriter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.Enumerator;
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

  public OneDBTableSchema getTableSchema(String tableName) {
    return schemaManager.getTableSchema(tableName);
  }

  public List<Pair<OwnerClient, String>> getTableClients(String tableName) {
    OneDBTableSchema table = getTableSchema(tableName);
    return table != null ? table.getTableList() : ImmutableList.of();
  }

  public DataSet executeQueryPlan(Plan plan) {
    LOG.info("plan before rewrite:\n{}", plan);
    plan = plan.rewrite(rewriter);
    LOG.info("plan after rewrite:\n{}", plan);
    return UserSideImplementor.getImplementor(plan, this).implement(plan);
  }

  /**
   * onedb query
   */
  public Enumerator<Row> oneDBQuery(long planId) {
    Plan plan = QueryPlanPool.getPlan(planId);
    // todo: support for choosing the appropriate rewriter
    return new EnumerableDataSet(executeQueryPlan(plan));
  }
}
