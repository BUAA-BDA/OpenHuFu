package com.hufudb.openhufu.owner.implementor.aggregate.sum;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.bristol.CircuitType;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.mpc.gmw.GMW;
import com.hufudb.openhufu.mpc.ot.PublicKeyOT;
import com.hufudb.openhufu.mpc.utils.Boardcast;
import com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregateFunction;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class GMWSum extends OwnerAggregateFunction {
  static final Logger LOG = LoggerFactory.getLogger(GMWSum.class);

  final GMW gmw;
  final Boardcast boardcast;

  GMWSum(int inputRef, GMW gmw, Boardcast boardcast, OpenHuFuPlan.TaskInfo taskInfo) {
    super(inputRef, null, taskInfo);
    this.gmw = gmw;
    this.boardcast = boardcast;
  }

  public GMWSum(OpenHuFuPlan.Expression agg, Rpc rpc, ExecutorService threadPool, OpenHuFuPlan.TaskInfo taskInfo) {
    this(agg.getIn(0).getI32(), new GMW(rpc, new PublicKeyOT(rpc), threadPool), new Boardcast(rpc), taskInfo);
  }

  @Override
  public Comparable aggregate() {
    try {
      List<byte[]> localShares = (List<byte[]>) gmw.run(super.taskInfo.getTaskId(), taskInfo.getPartiesList(), ImmutableList.of(OpenHuFuCodec.encodeInt(sum)), CircuitType.ADD_32.getId());
      int receiver = taskInfo.getParties(0);
      int sender = taskInfo.getParties(1);
      List<byte[]> remoteShares = (List<byte[]>) boardcast.run(taskInfo.getTaskId(), ImmutableList.of(sender, receiver), localShares);
      if (remoteShares.isEmpty()) {
        return null;
      } else {
        byte[] res = new byte[4];
        OpenHuFuCodec.xor(localShares.get(0), remoteShares.get(0), res);
        return OpenHuFuCodec.decodeInt(res);
      }
    } catch (ProtocolException e) {
      LOG.error("Error when executing GMW", e);
      return null;
    }
  }

  @Override
  public AggregateFunction<Row, Comparable> copy() {
    return new GMWSum(inputRef, gmw, boardcast, taskInfo);
  }
}
