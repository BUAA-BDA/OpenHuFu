package com.hufudb.onedb.rewriter;

import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.RootPlan;
import com.hufudb.onedb.plan.UnaryPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Rewriter {
  static final Logger LOG = LoggerFactory.getLogger(Rewriter.class);

  void rewriteChild(Plan Plan);
  Plan rewriteRoot(RootPlan root);
  Plan rewriteBinary(BinaryPlan binary);
  Plan rewriteUnary(UnaryPlan unary);
  Plan rewriteLeaf(LeafPlan leaf);
}
