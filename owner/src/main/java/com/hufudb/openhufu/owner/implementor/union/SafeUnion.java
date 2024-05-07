package com.hufudb.openhufu.owner.implementor.union;

import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.union.SecretUnion;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SafeUnion implements OwnerUnion {

  static final Logger LOG = LoggerFactory.getLogger(SafeUnion.class);

  @Override
  public DataSet union(DataSet in, Rpc rpc, OpenHuFuPlan.TaskInfo taskInfo) throws ProtocolException {
    LOG.info("using safe union.");
    SecretUnion secretUnion = new SecretUnion(rpc);
    return (DataSet) secretUnion.run(taskInfo.getTaskId(), taskInfo.getPartiesList(), in);
  }
}
