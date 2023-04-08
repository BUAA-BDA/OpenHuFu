package com.hufudb.openhufu.owner.implementor.knn;

import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;

public interface KNN {
    DataSet kNN(DataSet in, Rpc rpc, OpenHuFuPlan.TaskInfo taskInfo)
            throws ProtocolException;
}