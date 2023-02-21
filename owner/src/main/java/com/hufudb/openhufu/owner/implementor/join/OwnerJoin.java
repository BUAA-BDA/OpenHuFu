package com.hufudb.openhufu.owner.implementor.join;

import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;
import com.hufudb.openhufu.rpc.Rpc;

/**
 * @author yang.song
 * @date 2/16/23 7:07 PM
 */
public interface OwnerJoin {

  DataSet join(DataSet in, JoinCondition joinCond, boolean isLeft, Rpc rpc, TaskInfo taskInfo)
      throws ProtocolException;
}
