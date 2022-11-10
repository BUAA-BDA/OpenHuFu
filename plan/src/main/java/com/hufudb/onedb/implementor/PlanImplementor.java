package com.hufudb.onedb.implementor;

import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.UnaryPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PlanImplementor {
  static final Logger LOG = LoggerFactory.getLogger(PlanImplementor.class);

  DataSet implement(Plan plan);
  DataSet leafQuery(LeafPlan plan);
  DataSet unaryQuery(UnaryPlan plan);
  DataSet binaryQuery(BinaryPlan plan);
}
