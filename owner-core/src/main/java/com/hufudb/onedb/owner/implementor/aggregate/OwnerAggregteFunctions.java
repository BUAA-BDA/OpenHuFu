package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.aggregate.AggregateFunction;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.gmw.GMW;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.mpc.utils.Boardcast;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;

public class OwnerAggregteFunctions {
  public static AggregateFunctions getAggregateFunc(OneDBExpression exp, Rpc rpc, ExecutorService threadPool, TaskInfoProto taskInfo) {
    if (exp instanceof OneDBAggCall) {
      switch (((OneDBAggCall) exp).getAggType()) {
        case SUM:
          return new GMWSum((OneDBAggCall) exp, rpc, threadPool, taskInfo);
        default:
          throw new UnsupportedOperationException("Unsupported aggregate function");
      }
    } else {
      throw new UnsupportedOperationException("Just support single aggregate function");
    }
  }

  public static class GMWSum implements AggregateFunctions<Row, Comparable> {
    int sum;
    final int inputRef;
    final GMW gmw;
    final Boardcast boardcast;
    final TaskInfoProto taskInfo;

    GMWSum(int inputRef, GMW gmw, Boardcast boardcast, TaskInfoProto taskInfo) {
      this.sum = 0;
      this.inputRef = inputRef;
      this.gmw = gmw;
      this.boardcast = boardcast;
      this.taskInfo = taskInfo;
    }

    GMWSum(OneDBAggCall agg, Rpc rpc, ExecutorService threadPool, TaskInfoProto taskInfo) {
      this(agg.getInputRef().get(0), new GMW(rpc, new PublicKeyOT(rpc), threadPool), new Boardcast(rpc), taskInfo);
    }

    @Override
    public void add(Row ele) {
      Object e = ele.getObject(inputRef);
      sum += ((Number) e).intValue();
    }

    @Override
    public Comparable aggregate() {
      List<byte[]> localShares = gmw.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), ImmutableList.of(OneDBCodec.encodeInt(sum)), CircuitType.ADD_32.getId());
      List<byte[]> remoteShares = boardcast.run(taskInfo.getTaskId(), ImmutableList.of(taskInfo.getParties(1)), localShares, taskInfo.getParties(0));
      if (remoteShares.isEmpty()) {
        return null;
      } else {
        OneDBCodec.xor(localShares.get(0), remoteShares.get(0));
        return OneDBCodec.decodeInt(localShares.get(0));
      }
    }

    @Override
    public AggregateFunctions<Row, Comparable> patternCopy() {
      return new GMWSum(inputRef, gmw, boardcast, taskInfo);
    }
  }
}
