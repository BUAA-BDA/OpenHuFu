package com.hufudb.onedb.owner.adapter.json;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.LimitDataSet;
import com.hufudb.onedb.data.storage.SortedDataSet;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.json.jsonsrc.JsonSrcFactory;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonAdapter implements Adapter {
  protected final static Logger LOG = LoggerFactory.getLogger(JsonAdapter.class);

  final SchemaManager schemaManager;
  final Map<String, JsonTable> tables;

  public JsonAdapter(JsonSrcFactory jsonSrcFactory, AdapterConfig config) {
    tables = new HashMap<>();
    schemaManager = new SchemaManager();
    jsonSrcFactory.getTableNames(config).forEach(tableName -> {
      tables.put(tableName, new JsonTable(tableName, jsonSrcFactory.createJsonSrc(tableName)));
      schemaManager.addLocalTable(TableSchema.of(tableName, tables.get(tableName).getSchema()));
    });
  }

  @Override
  public SchemaManager getSchemaManager() {
    return schemaManager;
  }

  @Override
  public DataSet query(Plan queryPlan) {
    if (queryPlan instanceof LeafPlan) {
      return queryInternal((LeafPlan) queryPlan);
    } else {
      LOG.warn("Unsupported plan type {} for csv adapter", queryPlan.getPlanType());
      return EmptyDataSet.INSTANCE;
    }
  }

  @Override
  public void init() {
    // do nothing
  }


  @Override
  public void shutdown() {
    tables.clear();
  }

  Schema getOutSchema(String publishedTableName) {
    Schema schema = schemaManager.getPublishedSchema(publishedTableName);
    Schema.Builder builder = Schema.newBuilder();
    for (int i = 0; i < schema.size(); ++i) {
      builder.add("", schema.getType(i), schema.getModifier(i));
    }
    return builder.build();
  }

  DataSet queryInternal(LeafPlan plan) {
    String publishedTableName = plan.getTableName();
    String actualTableName = schemaManager.getActualTableName(publishedTableName);
    if (actualTableName.isBlank()) {
      LOG.error("Published table {} not found", publishedTableName);
      return EmptyDataSet.INSTANCE;
    }
    Schema schema = getOutSchema(publishedTableName);
    List<Integer> mappings = schemaManager.getPublishedSchemaMapping(publishedTableName);
    JsonTable target = tables.get(actualTableName);
    if (target == null) {
      LOG.error("CSV table {} not found", actualTableName);
      return EmptyDataSet.INSTANCE;
    }
    DataSet res = target.scanWithSchema(schema, mappings);
    if (!plan.getSelectExps().isEmpty()) {
      res = Interpreter.map(res, plan.getSelectExps());
    }
    if (!plan.getWhereExps().isEmpty()) {
      res = Interpreter.filter(res, plan.getWhereExps());
    }
    if (!plan.getAggExps().isEmpty()) {
      res = Interpreter.aggregate(res, plan.getGroups(), plan.getAggExps());
    }
    if (!plan.getOrders().isEmpty()) {
      res = SortedDataSet.sort(res, plan.getOrders());
    }
    if (plan.getFetch() != 0 || plan.getOffset() != 0) {
      res = LimitDataSet.limit(res, plan.getOffset(), plan.getFetch());
    }
    return res;
  }

}
