package com.hufudb.openhufu.owner.implementor.knn;

import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinarySearchKNN implements KNN {
    static final Logger LOG = LoggerFactory.getLogger(BinarySearchKNN.class);
    @Override
    public DataSet kNN(DataSet in, Rpc rpc, OpenHuFuPlan.TaskInfo taskInfo) throws ProtocolException {
        //todo
        LOG.info("Using binary-search KNN.");
        return in;
    }
}
