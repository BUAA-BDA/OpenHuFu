package com.hufudb.openhufu.core.client;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hufudb.openhufu.common.metrics.aspect.TimeCost;
import com.hufudb.openhufu.common.enums.TimeTerm;
import com.hufudb.openhufu.core.config.OpenHuFuConfig;
import com.hufudb.openhufu.core.data.EnumerableDataSet;
import com.hufudb.openhufu.core.implementor.UserSideImplementor;
import com.hufudb.openhufu.core.rewriter.BasicRewriter;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.core.table.OpenHuFuTableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.QueryPlanPool;
import com.hufudb.openhufu.rewriter.Rewriter;
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
public class OpenHuFuClient {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuClient.class);
  private final OpenHuFuSchemaManager schemaManager;
  final ExecutorService threadPool;
  private final AtomicInteger queryId;
  private final Rewriter rewriter;
  private String threadPoolNameFormat = "OpenHuFuClient-threadpool-%d";

  public OpenHuFuClient(OpenHuFuSchemaManager schemaManager) {
    this.schemaManager = schemaManager;
    this.threadPool = Executors.newFixedThreadPool(OpenHuFuConfig.CLIENT_THREAD_NUM,
        new ThreadFactoryBuilder().setNameFormat(threadPoolNameFormat).setDaemon(false)
            .build());
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

  public OpenHuFuTableSchema getTableSchema(String tableName) {
    return schemaManager.getTableSchema(tableName);
  }

  public List<Pair<OwnerClient, String>> getTableClients(String tableName) {
    OpenHuFuTableSchema table = getTableSchema(tableName);
    return table != null ? table.getTableList() : ImmutableList.of();
  }

  @TimeCost(term = TimeTerm.TOTAL_QUERY_TIME)
  public DataSet executeQueryPlan(Plan plan) {
    plan = plan.rewrite(rewriter);
    return UserSideImplementor.getImplementor(plan, this).implement(plan);
  }

  /**
   * openhufu query
   */
  public Enumerator<Row> openHuFuQuery(long planId) {
    Plan plan = QueryPlanPool.getPlan(planId);
    // todo: support for choosing the appropriate rewriter
    return new EnumerableDataSet(executeQueryPlan(plan));
  }
}
