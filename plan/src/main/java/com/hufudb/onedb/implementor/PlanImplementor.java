package com.hufudb.onedb.implementor;

import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.plan.Plan;

public interface PlanImplementor {
  DataSet implement(Plan plan);
  DataSet leafQuery(Plan plan);
  DataSet unaryQuery(Plan plan);
  DataSet binaryQuery(Plan plan);
}
