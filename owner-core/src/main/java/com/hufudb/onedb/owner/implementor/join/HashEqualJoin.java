package com.hufudb.onedb.owner.implementor.join;

import java.util.ArrayList;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashEqualJoin {
  static final Logger LOG = LoggerFactory.getLogger(HashEqualJoin.class);


  public static QueryableDataSet apply(OwnerQueryableDataSet in, OneDBJoinInfo joinInfo, Rpc rpc, TaskInfoProto taskInfo, boolean isLeft) {
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
    int senderId;
    int receiverId;
    // At present, the left party collect result
    // todo: choose collector by data size
    if (isLeft) {
      assert parties.get(0) == rpc.ownParty().getPartyId();
      joinKey = in.encode(joinInfo.getLeftKeys());
      receiverId = parties.get(0);
      senderId = parties.get(1);
    } else {
      assert parties.get(1) == rpc.ownParty().getPartyId();
      joinKey = in.encode(joinInfo.getRightKeys());
      receiverId = parties.get(0);
      senderId = parties.get(1);
    }
    List<byte[]> res = psi.run(taskInfo.getTaskId(), parties, joinKey, HashFunction.SHA256.getId());
    List<Row> newRows = new ArrayList<>();
    List<Row> oldRows = in.getRows();
    for (byte[] r : res) {
      int rowId = OneDBCodec.decodeInt(r);
      newRows.add(oldRows.get(rowId));
    }
    if (senderId == rpc.ownParty().getPartyId()) {
      return senderProcedure();
    } else {
      return receiverProcedure();
    }
  }

  static QueryableDataSet senderProcedure() {
    return null;
  }

  static QueryableDataSet receiverProcedure() {
    return null;
  }
}
