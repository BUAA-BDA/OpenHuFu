package com.hufudb.openhufu.owner.implementor.aggregate.sum;

import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.secretsharing.SecretSharing;
import com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregateFunction;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class SecretSharingSum extends OwnerAggregateFunction {
  static final Logger LOG = LoggerFactory.getLogger(SecretSharingSum.class);
  private int sum;
  final private SecretSharing ss;
  final private boolean hasOutput;

  SecretSharingSum(int inputRef, SecretSharing ss, OpenHuFuData.ColumnType type, OpenHuFuPlan.TaskInfo taskInfo) {
    super(inputRef, type, taskInfo);
    this.sum = 0;
    this.ss = ss;
    this.hasOutput = ss.getOwnId() == taskInfo.getPartiesList().get(0);
  }

  public SecretSharingSum(OpenHuFuPlan.Expression agg, Rpc rpc, ExecutorService threadPool, OpenHuFuPlan.TaskInfo taskInfo) {
    this(agg.getIn(0).getI32(), new SecretSharing(rpc), agg.getOutType(), taskInfo);
  }

  @Override
  public Comparable aggregate() {
    try {
      Object res = ss.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), type, sum, OpenHuFuPlan.OperatorType.PLUS);
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
      LOG.error("Error when executing secretsharing");
      return null;
    }
  }

  @Override
  public void add(Row ele) {
    Object e = ele.get(inputRef);
    sum += ((Number) e).intValue();
  }
  @Override
  public AggregateFunction<Row, Comparable> copy() {
    return new SecretSharingSum(inputRef, ss, type, taskInfo);
  }

}
