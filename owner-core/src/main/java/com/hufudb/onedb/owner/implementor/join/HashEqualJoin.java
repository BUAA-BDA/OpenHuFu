package com.hufudb.onedb.owner.implementor.join;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.mpc.codec.HashFunction;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.psi.HashPSI;
import com.hufudb.onedb.mpc.utils.Stream;
import com.hufudb.onedb.owner.implementor.OwnerQueryableDataSet;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashEqualJoin {
  static final Logger LOG = LoggerFactory.getLogger(HashEqualJoin.class);

  static List<byte[]> encode(List<Integer> keys, QueryableDataSet dataSet) {
    final List<Row> rows = dataSet.getRows();
    final int keySize = keys.size();
    List<byte[]> result = new ArrayList<>();
    for (Row row : rows) {
      Row.RowBuilder builder = Row.newBuilder(keySize);
      for (int i = 0; i < keySize; ++i) {
        builder.set(i, row.getObject(keys.get(i)));
      }
      Row k = builder.build();
      result.add(SerializationUtils.serialize(k));
    }
    return result;
  }

  static List<byte[]> encode(QueryableDataSet dataSet) {
    final List<Row> rows = dataSet.getRows();
    List<byte[]> result = new ArrayList<>();
    for (Row row : rows) {
      result.add(SerializationUtils.serialize(row));
    }
    return result;
  }

  static List<Row> decode(List<byte[]> payload) {
    List<Row> rows = new ArrayList<>();
    for (byte[] b : payload) {
      rows.add((Row) SerializationUtils.deserialize(b));
    }
    return rows;
  }

  public static QueryableDataSet apply(QueryableDataSet in, OneDBJoinInfo joinInfo, Rpc rpc,
      TaskInfoProto taskInfo, Header outputHeader) {
    if (!joinInfo.getConditions().isEmpty()) {
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
    // todo: choose collector by data size
    if (joinInfo.isLeft()) {
      assert parties.get(0) == rpc.ownParty().getPartyId();
      joinKey = encode(joinInfo.getLeftKeys(), in);
    } else {
      assert parties.get(1) == rpc.ownParty().getPartyId();
      joinKey = encode(joinInfo.getRightKeys(), in);
    }
    List<byte[]> res = psi.run(taskInfo.getTaskId(), parties, joinKey, HashFunction.SHA256.getId());
    LOG.debug("Get {} rows in HashPSI", res.size());
    List<Row> newRows = new ArrayList<>();
    List<Row> oldRows = in.getRows();
    for (byte[] r : res) {
      int rowId = OneDBCodec.decodeInt(r);
      newRows.add(oldRows.get(rowId));
    }
    oldRows.clear();
    oldRows.addAll(newRows);
    QueryableDataSet output = new OwnerQueryableDataSet(outputHeader);
    // return directly if no intersection
    if (res.size() == 0) {
      return output;
    }
    if (senderId == rpc.ownParty().getPartyId()) {
      return senderProcedure(in, taskInfo.getTaskId(), senderId, receiverId, st, output);
    } else if (receiverId == rpc.ownParty().getPartyId()) {
      return receiverProcedure(in, taskInfo.getTaskId(), senderId, receiverId, st, output);
    } else {
      LOG.error("{} not found in participants of HashEqualJoin", rpc.ownParty());
      throw new RuntimeException("Not participant of HashEqualJoin");
    }
  }

  static QueryableDataSet senderProcedure(QueryableDataSet in, long taskId, int senderId,
      int receiverId, Stream stream, QueryableDataSet out) {
    List<byte[]> inputs = encode(in);
    stream.run(taskId, ImmutableList.of(receiverId), inputs, senderId);
    in.getRows().clear();
    return out;
  }

  static QueryableDataSet receiverProcedure(QueryableDataSet in, long taskId, int senderId,
      int receiverId, Stream stream, QueryableDataSet out) {
    List<Row> localRows = in.getRows();
    List<byte[]> result =
        stream.run(taskId, ImmutableList.of(receiverId), ImmutableList.of(), senderId);
    List<Row> remoteRows = decode(result);
    if (localRows.size() != remoteRows.size()) {
      LOG.error("Row number not match in hash equal join: local[{}], remote[{}]", localRows.size(),
          remoteRows.size());
      throw new RuntimeException("Row number not match in hash equal join");
    } else if (localRows.size() == 0) {
      return out;
    }
    for (int i = 0; i < localRows.size(); ++i) {
      out.addRow(Row.merge(localRows.get(i), remoteRows.get(i)));
    }
    return out;
  }
}
