package com.hufudb.onedb.owner.adapter;

import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.owner.schema.SchemaManager;

public interface Adapter {
  SchemaManager getSchemaManager();
  void query(OneDBContext queryContext, DataSet dataSet);
  void init();
  void shutdown();
}
