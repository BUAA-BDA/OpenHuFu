package com.hufudb.openhufu.rewriter;

import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.RootPlan;
import com.hufudb.openhufu.plan.UnaryPlan;
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
