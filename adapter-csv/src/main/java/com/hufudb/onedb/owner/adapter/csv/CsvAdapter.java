package com.hufudb.onedb.owner.adapter.csv;

import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.owner.adapter.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * load all .csv file in csvDir
 */
public class CsvAdapter implements Adapter {
  protected final static Logger LOG = LoggerFactory.getLogger(CsvAdapter.class);

  SchemaManager schemaManager;
  Map<String, CsvDataSet> tables;

  public CsvAdapter(String csvDir) {
    tables = new HashMap<>();
    try {
      loadTables(csvDir);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void loadTables(String csvDir) throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(csvDir))) {
      stream.filter(file -> !Files.isDirectory(file))
          .filter(file -> file.toString().endsWith(".csv")).forEach(file -> {
            try {
              String fileName = file.getFileName().toString();
              String tableName = fileName.substring(0, fileName.length() - 4);
              tables.put(tableName, new CsvDataSet(file));
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
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
    for (CsvDataSet dataSet : tables.values()) {
      dataSet.close();
    }
    tables = ImmutableMap.of();
  }

  DataSet queryInternal(LeafPlan plan) {
    String publishedTableName = plan.getTableName();
    String actualTableName = schemaManager.getActualTableName(publishedTableName);
    if (actualTableName.isBlank()) {
      LOG.error("Published table {} not found", publishedTableName);
      return EmptyDataSet.INSTANCE;
    }
    Schema schema = schemaManager.getPublishedSchema(publishedTableName);
    CsvDataSet target = tables.get(actualTableName);
    if (target == null) {
      LOG.error("CSV table {} not found", actualTableName);
      return EmptyDataSet.INSTANCE;
    }
    DataSet res = Interpreter.map(target, plan.getSelectExps());
    return res;
  }
}
