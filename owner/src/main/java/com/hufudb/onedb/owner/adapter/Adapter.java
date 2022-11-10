package com.hufudb.onedb.owner.adapter;

import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.plan.Plan;

public interface Adapter {
  SchemaManager getSchemaManager();
  DataSet query(Plan queryPlan);
  void init();
  void shutdown();
}
