package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.mpc.ProtocolException;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.gmw.GMW;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.mpc.secretsharing.SecretSharing;
import com.hufudb.onedb.mpc.utils.Boardcast;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerAggregateFunctions {
  static final Logger LOG = LoggerFactory.getLogger(OwnerAggregateFunctions.class);

  public static AggregateFunction getAggregateFunc(Expression exp, Rpc rpc, ExecutorService threadPool, TaskInfo taskInfo) {
    int partyNum = taskInfo.getPartiesCount();
    if (exp.getOpType().equals(OperatorType.AGG_FUNC)) {
      if (partyNum == 2) {
        switch (AggFuncType.of(exp.getI32())) {
          case SUM:
            return new GMWSum(exp, rpc, threadPool, taskInfo);
          default:
            throw new UnsupportedOperationException("Unsupported aggregate function");
        }
      } else {
        switch (AggFuncType.of(exp.getI32())) {
          case SUM:
            return new SecretSharingSum(exp, rpc, exp.getOutType(),threadPool, taskInfo);
          default:
            throw new UnsupportedOperationException("Unsupported aggregate function");
        }
      }
    } else {
      throw new UnsupportedOperationException("Just support single aggregate function");
    }
  }

  public static class SecretSharingSum implements AggregateFunction<Row, Comparable> {
    int sum;
    final int inputRef;
    final SecretSharing ss;
    final ColumnType type;
    final TaskInfo taskInfo;
    final boolean hasOutput;

    SecretSharingSum(int inputRef, SecretSharing ss, ColumnType type, TaskInfo taskInfo) {
      this.sum = 0;
      this.inputRef = inputRef;
      this.ss = ss;
      this.type = type;
      this.taskInfo = taskInfo;
      this.hasOutput = ss.getOwnId() == taskInfo.getPartiesList().get(0);
    }

    SecretSharingSum(Expression agg, Rpc rpc, ColumnType type, ExecutorService threadPool, TaskInfo taskInfo) {
      this(agg.getIn(0).getI32(), new SecretSharing(rpc), type, taskInfo);
    }

    @Override
    public void add(Row ele) {
      Object e = ele.get(inputRef);
      sum += ((Number) e).intValue();
    }

    @Override
    public Comparable aggregate() {
      try {
        Object res = ss.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), type, sum, OperatorType.PLUS);
        if (!hasOutput) {
          return null;
        }
        switch (type) {
          case DOUBLE:
            return ((Number) res).doubleValue();
          case FLOAT:
            return ((Number) res).floatValue();
          case BYTE:
          case SHORT:
          case INT:
            return ((Number) res).intValue();
          case LONG:
            return ((Number) res).longValue();
          default:
            throw new UnsupportedOperationException("Unsupported type for secretsharing sum");
        }
      } catch (ProtocolException e) {
        LOG.error("Error when executing secretsharing: {}", e.getMessage());
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public AggregateFunction<Row, Comparable> copy() {
      return new SecretSharingSum(inputRef, ss, type, taskInfo);
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
      try {
        List<byte[]> localShares = (List<byte[]>) gmw.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), ImmutableList.of(OneDBCodec.encodeInt(sum)), CircuitType.ADD_32.getId());
        int receiver = taskInfo.getParties(0);
        int sender = taskInfo.getParties(1);
        List<byte[]> remoteShares = (List<byte[]>) boardcast.run(taskInfo.getTaskId(), ImmutableList.of(sender, receiver), localShares);
        if (remoteShares.isEmpty()) {
          return null;
        } else {
          byte[] res = new byte[4];
          OneDBCodec.xor(localShares.get(0), remoteShares.get(0), res);
          return OneDBCodec.decodeInt(res);
        }
      } catch (ProtocolException e) {
        LOG.error("Error when executing GMW: {}", e.getMessage());
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public AggregateFunction<Row, Comparable> copy() {
      return new GMWSum(inputRef, gmw, boardcast, taskInfo);
    }
  }
}
