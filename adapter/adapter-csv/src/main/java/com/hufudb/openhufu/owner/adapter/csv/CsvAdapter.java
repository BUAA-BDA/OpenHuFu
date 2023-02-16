package com.hufudb.openhufu.owner.adapter.csv;

import com.hufudb.openhufu.owner.adapter.AdapterConfig;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.EmptyDataSet;
import com.hufudb.openhufu.data.storage.LimitDataSet;
import com.hufudb.openhufu.data.storage.SortedDataSet;
import com.hufudb.openhufu.interpreter.Interpreter;
import com.hufudb.openhufu.owner.adapter.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * load all .csv file in csvDir
 */
public class CsvAdapter implements Adapter {
  protected final static Logger LOG = LoggerFactory.getLogger(CsvAdapter.class);

  final SchemaManager schemaManager;
  final Map<String, CsvTable> tables;

  public CsvAdapter(AdapterConfig config) {
    tables = new HashMap<>();
    schemaManager = new SchemaManager();
    try {
      loadTables(config.url, config.delimiter);
    } catch (IOException e) {
      LOG.error("Load tables error", e);
    }
  }

  void loadTables(String csvDir, String delimiter) throws IOException {
    File base = new File(csvDir);
    if (base.isDirectory()) {
      try (Stream<Path> stream = Files.list(base.toPath())) {
        stream.filter(file -> !Files.isDirectory(file))
            .filter(file -> file.toString().endsWith(".csv")).forEach(file -> {
              try {
                String fileName = file.getFileName().toString();
                String tableName = fileName.substring(0, fileName.length() - 4);
                CsvTable table = new CsvTable(tableName, file, delimiter);
                tables.put(tableName, table);
                schemaManager.addLocalTable(TableSchema.of(tableName, table.getSchema()));
              } catch (IOException e) {
                LOG.error("Parse file: {} error", file.getFileName(), e);
              }
            });
      }
    } else if (base.getName().endsWith(".csv")) {
      String fileName = base.getName();
      String tableName = fileName.substring(0, fileName.length() - 4);
      CsvTable table = new CsvTable(tableName, base.toPath(), delimiter);
      tables.put(tableName, table);
      schemaManager.addLocalTable(TableSchema.of(tableName, table.getSchema()));
    }
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
    CsvTable target = tables.get(actualTableName);
    if (target == null) {
      LOG.error("CSV table {} not found", actualTableName);
      return EmptyDataSet.INSTANCE;
    }
    DataSet res = target.scanWithSchema(schema, mappings);
    if (!plan.getWhereExps().isEmpty()) {
      res = Interpreter.filter(res, plan.getWhereExps());
    }
    if (!plan.getSelectExps().isEmpty()) {
      res = Interpreter.map(res, plan.getSelectExps());
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
