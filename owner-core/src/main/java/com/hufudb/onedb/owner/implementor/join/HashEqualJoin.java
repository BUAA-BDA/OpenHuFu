package com.hufudb.onedb.owner.implementor.join;

import java.util.ArrayList;
import java.util.List;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.mpc.codec.HashFunction;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.psi.HashPSI;
import com.hufudb.onedb.owner.implementor.OwnerQueryableDataSet;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashEqualJoin {
  static final Logger LOG = LoggerFactory.getLogger(HashEqualJoin.class);


  public static QueryableDataSet apply(OwnerQueryableDataSet in, OneDBJoinInfo joinInfo, HashPSI psi, TaskInfoProto taskInfo, boolean isLeft) {
    if (!joinInfo.getConditions().isEmpty()) {
      LOG.error("HashEqualJoin not support theta join");
    }
    List<byte[]> joinKey;
    if (isLeft) {
      joinKey = in.encode(joinInfo.getLeftKeys());
    } else {
      joinKey = in.encode(joinInfo.getRightKeys());
    }
    List<byte[]> res = psi.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), joinKey, HashFunction.SHA256.getId());
    List<Row> newRows = new ArrayList<>();
    List<Row> oldRows = in.getRows();
    for (byte[] r : res) {
      int rowId = OneDBCodec.decodeInt(r);
      newRows.add(oldRows.get(rowId));
    }
    
    return in;
  }
}
