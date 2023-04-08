package com.hufudb.openhufu.mpc.lib.aby;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.mpc.RpcProtocolExecutor;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiCmp extends AbyWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(MultiCmp.class);

    public MultiCmp(List<OpenHuFuRpc> rpcs, int me, List<Integer> ids, String address, int port) {
        super(rpcs, me, ids, address, port);
    }

    @Override
    public Object run(long taskId, List<Integer> parties, Object input, OperatorType op) throws ProtocolException {
        if (me == 0) {
            List<Integer> abyIds = ImmutableList.of(0, 1);
            int curMax = (int)input;
            for (int id = 1; id < num; id++) {
                int curPort = this.port;
                ProtocolExecutor aby = factory.create(
                    OwnerInfo.newBuilder().setEndpoint(address + ":" + curPort).setId(me).build(), ProtocolType.ABY);
                List<byte[]> abyInputs = ImmutableList.of(OpenHuFuCodec.encodeInt(curMax), OpenHuFuCodec.encodeInt(0));
                List<byte[]> abyOutputs = (List<byte[]>) aby.run(taskId, abyIds, abyInputs, op, ColumnType.INT, address, curPort, id != num - 1, (int)taskId * 100 + id);
                curMax = OpenHuFuCodec.decodeInt(abyOutputs.get(0));
                LOG.info("round {}: {}", id, Integer.toHexString(curMax));
            }
            return curMax;
        }
        List<Integer> abyIds = ImmutableList.of(1, 0);
        if (me == 1 && num == 2) {
            ProtocolExecutor aby = factory.create(
                OwnerInfo.newBuilder().setEndpoint(address + ":" + port).setId(me).build(), ProtocolType.ABY);
            List<byte[]> abyInputs = ImmutableList.of(OpenHuFuCodec.encodeInt(0), OpenHuFuCodec.encodeInt((int)input));
            List<byte[]> abyOutputs = (List<byte[]>) aby.run(taskId, abyIds, abyInputs, op, ColumnType.INT, address, port, false, (int)taskId * 100 + me);
            int ret = OpenHuFuCodec.decodeInt(abyOutputs.get(0));
            return ret;
        }

        int val1 = 0, val2 = (int)input;
        if (me != 1) {
            List<byte[]> recv = rpcRecv(taskId, me - 1, me - 1);
            val1 = OpenHuFuCodec.decodeInt(recv.get(0));
            LOG.info("{} recv {}", me, Integer.toHexString(val1));
        }
        ProtocolExecutor aby = factory.create(
            OwnerInfo.newBuilder().setEndpoint(address + ":" + port).setId(me).build(), ProtocolType.ABY);
        List<byte[]> abyInputs = ImmutableList.of(OpenHuFuCodec.encodeInt(val1), OpenHuFuCodec.encodeInt(val2));
        List<byte[]> abyOutputs = (List<byte[]>) aby.run(taskId, abyIds, abyInputs, op, ColumnType.INT, address, port, me != num - 1, (int)taskId * 100 + me);
        val1 = OpenHuFuCodec.decodeInt(abyOutputs.get(0));
        if (me != num - 1) {
            byte[] send = OpenHuFuCodec.encodeInt(val1);
            rpcSend(taskId, me, me + 1, ImmutableList.of(send));
            LOG.info("{} send {}", me, Integer.toHexString(val1));
        }
        return 0;
    }
}
