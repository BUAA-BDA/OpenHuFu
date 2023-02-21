package com.hufudb.openhufu.owner.implementor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.EmptyDataSet;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.interpreter.Interpreter;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregation;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.rpc.Rpc;

public class OwnerSideImplementor implements PlanImplementor {
  Rpc rpc;
  Adapter dataSourceAdapter;
  ExecutorService threadPool;

  public OwnerSideImplementor(Rpc rpc, Adapter adapter, ExecutorService threadPool) {
    this.rpc = rpc;
    this.dataSourceAdapter = adapter;
    this.threadPool = threadPool;
  }

  @Override
  public DataSet implement(Plan context) {
    return context.implement(this);
  }

  @Override
  public DataSet binaryQuery(BinaryPlan binary) {
    List<Plan> children = binary.getChildren();
    assert children.size() == 2;
    Plan left = children.get(0);
    Plan right = children.get(1);
    DataSet in;
    boolean isLeft = true;
    if (left.getPlanType().equals(PlanType.EMPTY)) {
      // right
      in = right.implement(this);
      isLeft = false;
    } else if (right.getPlanType().equals(PlanType.EMPTY)) {
      // left
      in = left.implement(this);
    } else {
      LOG.error("Not support two side on a single owner yet");
      throw new UnsupportedOperationException("Not support two side on a single owner yet");
    }
    try {
      DataSet result =
          OwnerImplementorFactory.getJoin().join(in, binary.getJoinCond(), isLeft, rpc, binary.getTaskInfo());
      if (!binary.getSelectExps().isEmpty()) {
        result = Interpreter.map(result, binary.getSelectExps());
      }
      return result;
    } catch (ProtocolException e) {
      LOG.error("Error in HashPSIJoin: {}", e.getMessage());
      return EmptyDataSet.INSTANCE;
    }
  }

  @Override
  public DataSet unaryQuery(UnaryPlan unary) {
    List<Plan> children = unary.getChildren();
    assert children.size() == 1;
    DataSet input = implement(children.get(0));
    if (!unary.getSelectExps().isEmpty()) {
      input = Interpreter.map(input, unary.getSelectExps());
    }
    if (!unary.getAggExps().isEmpty()) {
      input = OwnerAggregation.aggregate(input, unary.getGroups(), unary.getAggExps(),
          children.get(0).getOutTypes(), rpc, threadPool, unary.getTaskInfo());
    }
    return input;
  }

  @Override
  public DataSet leafQuery(LeafPlan leaf) {
    try {
      return dataSourceAdapter.query(leaf);
    } catch (Exception e) {
      LOG.error("Error when execute query on Database", e);
      return EmptyDataSet.INSTANCE;
    }
  }
}
