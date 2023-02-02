package com.hufudb.openhufu.owner.adapter;

import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.plan.Plan;

public interface Adapter {
  SchemaManager getSchemaManager();
  DataSet query(Plan queryPlan);
  void init();
  void shutdown();
}
