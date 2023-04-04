package com.hufudb.openhufu.mpc.lib.aby;


import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolFactory;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.mpc.RpcProtocolExecutor;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiCmp {
    private int me;
    private int num;
    private String address;
    private int port;
    private ProtocolFactory factory;
    private List<Integer> ids = new ArrayList<>();
    private Rpc rpc;
    private static final Logger LOG = LoggerFactory.getLogger(MultiCmp.class);

    public MultiCmp(Rpc rpc, int me, int num, String address, int port, ProtocolFactory factory) {
        this.rpc = rpc;
        this.me = me;
        this.num = num;
        this.address = address;
        this.port = port;
        this.factory = factory;
        ids.add(me);
    }

    int runMultiCmp(int taskId, int val) throws ProtocolException {
        LOG.info("run CMP: {}, {}, {}", me, taskId, val);
        if (me == 0) {
            int curMax = val;
            for (int id = 1; id < num; id++) {
                int curPort = this.port + id;
                ProtocolExecutor aby = factory.create(
                    OwnerInfo.newBuilder().setEndpoint(address + ":" + curPort).setId(me).build(), ProtocolType.ABY);
                List<byte[]> input = ImmutableList.of(OpenHuFuCodec.encodeInt(curMax), OpenHuFuCodec.encodeInt(0));
                List<byte[]> tmp = (List<byte[]>) aby.run(taskId, ids, input, OperatorType.MAX, ColumnType.INT, 
                    this.address, curPort, id != num - 1);
                curMax = OpenHuFuCodec.decodeInt(tmp.get(0));
                LOG.info("round {}: {}", id, curMax);
            }
            return curMax;
        }
        if (me == 1 && num == 2) {
            ProtocolExecutor aby = factory.create(
                OwnerInfo.newBuilder().setEndpoint(address + ":" + port).setId(me).build(), ProtocolType.ABY);
            List<byte[]> input = ImmutableList.of(OpenHuFuCodec.encodeInt(0), OpenHuFuCodec.encodeInt(val));
            List<byte[]> tmp = (List<byte[]>) aby.run(taskId, ids, input, OperatorType.MAX, ColumnType.INT, 
                this.address, this.port, false);
            int ret = OpenHuFuCodec.decodeInt(tmp.get(0));
            return ret;
        }
        if (me == 1) {
            // TODO: multiparty
            // ProtocolExecutor aby = factory.create(
            //     OwnerInfo.newBuilder().setEndpoint(address + port).setId(me).build(), ProtocolType.ABY);
        }
        // TODO: multiparty
        return 0;
    }
}
