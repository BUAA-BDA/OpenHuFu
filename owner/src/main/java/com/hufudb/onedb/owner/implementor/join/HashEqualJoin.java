package com.hufudb.onedb.owner.implementor.join;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.HorizontalDataSet;
import com.hufudb.onedb.data.storage.MaterializedDataSet;
import com.hufudb.onedb.data.storage.ProtoDataSet;
import com.hufudb.onedb.data.storage.ProtoRowDataSet;
import com.hufudb.onedb.data.storage.VerticalDataSet;
import com.hufudb.onedb.mpc.codec.HashFunction;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.psi.HashPSI;
import com.hufudb.onedb.mpc.utils.Stream;
import com.hufudb.onedb.proto.DataPacket.PayloadProto;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.proto.OneDBData.RowsProto;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashEqualJoin {
  static final int MAX_SIZE = 1000;
  static final Logger LOG = LoggerFactory.getLogger(HashEqualJoin.class);

  static List<byte[]> encode(List<Integer> keys, DataSet dataSet) {
    ProtoRowDataSet keySets = ProtoRowDataSet.project(dataSet, keys);
    return keySets.toBytes();
  }

  static List<byte[]> encode(DataSet source) {
    return ProtoDataSet.slice(source, MAX_SIZE).stream().map(proto -> proto.toByteArray())
        .collect(Collectors.toList());
  }

  static MaterializedDataSet decode(List<byte[]> payload) {
    try {
      List<ProtoDataSet> dataSets = new ArrayList<>();
      for (byte[] b : payload) {
        dataSets.add(ProtoDataSet.create(DataSetProto.parseFrom(b)));
      }
      return new HorizontalDataSet(dataSets);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Fail to parse dataset in receiver: %d", e.getMessage());
      throw new RuntimeException("Fail to parse dataset in receiver");
    }
  }

  public static DataSet join(DataSet in, JoinCondition joinCond, Rpc rpc, TaskInfo taskInfo) {
    if (joinCond.hasCondition()) {
      LOG.error("HashEqualJoin not support theta join");
      throw new UnsupportedOperationException("HashEqualJoin not support theta join");
    }
    // note: require the party of left table precede the party of right table
    List<Integer> parties = taskInfo.getPartiesList();
    if (parties.size() != 2) {
      LOG.error("HashEqualJoin only support two parties");
      throw new UnsupportedOperationException("HashEqualJoin only support two parties");
    }
    HashPSI psi = new HashPSI(rpc);
    Stream st = new Stream(rpc);
    List<byte[]> joinKey;
    // At present, the left party collect result
    int receiverId = parties.get(0);
    int senderId = parties.get(1);
    MaterializedDataSet localSet = ProtoDataSet.materialize(in);
    // todo: choose collector by data size
    if (joinCond.getIsLeft()) {
      assert parties.get(0) == rpc.ownParty().getPartyId();
      long startTime = System.currentTimeMillis();
      joinKey = encode(joinCond.getLeftKeyList(), localSet);
      long endTime = System.currentTimeMillis();
      LOG.info("Left encode use {} ms", endTime - startTime);
    } else {
      assert parties.get(1) == rpc.ownParty().getPartyId();
      long startTime = System.currentTimeMillis();
      joinKey = encode(joinCond.getRightKeyList(), localSet);
      long endTime = System.currentTimeMillis();
      LOG.info("Right encode use {} ms", endTime - startTime);
    }
    List<byte[]> res = psi.run(taskInfo.getTaskId(), parties, joinKey, HashFunction.SHA256.getId());
    LOG.debug("Get {} rows in HashPSI", res.size());
    if (res.size() == 0) {
      return EmptyDataSet.INSTANCE;
    }
    localSet = new JoinFilterDataSet(localSet,
        res.stream().map(b -> OneDBCodec.decodeInt(b)).collect(Collectors.toList()));
    if (senderId == rpc.ownParty().getPartyId()) {
      return senderProcedure(localSet, taskInfo.getTaskId(), senderId, receiverId, st);
    } else if (receiverId == rpc.ownParty().getPartyId()) {
      return receiverProcedure(localSet, taskInfo.getTaskId(), senderId, receiverId, st);
    } else {
      LOG.error("{} not found in participants of HashEqualJoin", rpc.ownParty());
      throw new RuntimeException("Not participant of HashEqualJoin");
    }
  }

  static DataSet senderProcedure(MaterializedDataSet in, long taskId, int senderId, int receiverId,
      Stream stream) {
    List<byte[]> inputs = encode(in);
    stream.run(taskId, ImmutableList.of(receiverId), inputs, senderId);
    return EmptyDataSet.INSTANCE;
  }

  static DataSet receiverProcedure(MaterializedDataSet localDataSet, long taskId, int senderId, int receiverId,
      Stream stream) {
    List<byte[]> result =
        stream.run(taskId, ImmutableList.of(receiverId), ImmutableList.of(), senderId);
    MaterializedDataSet remoteDataSet = decode(result);
    if (remoteDataSet.rowCount() == 0) {
      return EmptyDataSet.INSTANCE;
    } else {
      return VerticalDataSet.create(localDataSet, remoteDataSet);
    }
  }
}
