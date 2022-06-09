package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.gmw.GMW;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.mpc.utils.Boardcast;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rpc.Rpc;

public class OwnerAggregteFunctions {
  public static AggregateFunction getAggregateFunc(Expression exp, Rpc rpc, ExecutorService threadPool, TaskInfo taskInfo) {
    if (exp.getOpType().equals(OperatorType.AGG_FUNC)) {
      switch (AggFuncType.of(exp.getI32())) {
        case SUM:
          return new GMWSum(exp, rpc, threadPool, taskInfo);
        default:
          throw new UnsupportedOperationException("Unsupported aggregate function");
      }
    } else {
      throw new UnsupportedOperationException("Just support single aggregate function");
    }
  }

  public static class GMWSum implements AggregateFunction<Row, Comparable> {
    int sum;
    final int inputRef;
    final GMW gmw;
    final Boardcast boardcast;
    final TaskInfo taskInfo;

    GMWSum(int inputRef, GMW gmw, Boardcast boardcast, TaskInfo taskInfo) {
      this.sum = 0;
      this.inputRef = inputRef;
      this.gmw = gmw;
      this.boardcast = boardcast;
      this.taskInfo = taskInfo;
    }

    GMWSum(Expression agg, Rpc rpc, ExecutorService threadPool, TaskInfo taskInfo) {
      this(agg.getIn(0).getI32(), new GMW(rpc, new PublicKeyOT(rpc), threadPool), new Boardcast(rpc), taskInfo);
    }

    @Override
    public void add(Row ele) {
      Object e = ele.get(inputRef);
      sum += ((Number) e).intValue();
    }

    @Override
    public Comparable aggregate() {
      List<byte[]> localShares = gmw.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), ImmutableList.of(OneDBCodec.encodeInt(sum)), CircuitType.ADD_32.getId());
      List<byte[]> remoteShares = boardcast.run(taskInfo.getTaskId(), ImmutableList.of(taskInfo.getParties(1)), localShares, taskInfo.getParties(0));
      if (remoteShares.isEmpty()) {
        return null;
      } else {
        byte[] res = new byte[4];
        OneDBCodec.xor(localShares.get(0), remoteShares.get(0), res);
        return OneDBCodec.decodeInt(res);
      }
    }

    @Override
    public AggregateFunction<Row, Comparable> copy() {
      return new GMWSum(inputRef, gmw, boardcast, taskInfo);
    }
  }
}
