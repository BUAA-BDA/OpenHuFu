package com.hufudb.onedb.owner.implementor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.mpc.ProtocolException;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.implementor.aggregate.OwnerAggregation;
import com.hufudb.onedb.owner.implementor.join.HashEqualJoin;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.UnaryPlan;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.rpc.Rpc;

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
      // only right
      in = right.implement(this);
      isLeft = false;
    } else if (right.getPlanType().equals(PlanType.EMPTY)) {
      // only left
      in = left.implement(this);
    } else {
      LOG.error("Not support two side on a single owner yet");
      throw new UnsupportedOperationException("Not support two side on a single owner yet");
    }
    try {
      DataSet result =
          HashEqualJoin.join(in, binary.getJoinCond(), isLeft, rpc, binary.getTaskInfo());
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
      LOG.error("Error when execute query on Database");
      e.printStackTrace();
      return EmptyDataSet.INSTANCE;
    }
  }
}
