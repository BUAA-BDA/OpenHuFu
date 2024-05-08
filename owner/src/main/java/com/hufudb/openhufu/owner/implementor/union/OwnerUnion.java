package com.hufudb.openhufu.owner.implementor.union;

import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;
import com.hufudb.openhufu.rpc.Rpc;


public interface OwnerUnion {

  DataSet union(DataSet in, Rpc rpc, TaskInfo taskInfo)
      throws ProtocolException;
}
