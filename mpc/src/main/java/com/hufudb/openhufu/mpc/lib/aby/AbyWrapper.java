package com.hufudb.openhufu.mpc.lib.aby;

import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import com.hufudb.openhufu.mpc.ProtocolFactory;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;
import com.hufudb.openhufu.mpc.lib.LibraryLoader;
import com.hufudb.openhufu.mpc.ProtocolException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbyWrapper {
    protected int me;
    protected int num;
    protected String address;
    protected int port;
    protected static ProtocolFactory factory = null;
    private List<Integer> ids;
    private List<OpenHuFuRpc> rpcs;
    private static final Logger LOG = LoggerFactory.getLogger(AbyWrapper.class);

    static void loadAby() {
        if (factory != null) {
            return;
        }
        String openhufuRoot = System.getenv("OPENHUFU_ROOT");
        Path libDir = Paths.get(openhufuRoot, "lib");
        Map<ProtocolType, ProtocolFactory> factories =
            LibraryLoader.loadProtocolLibrary(libDir.toString());
        factory = factories.get(ProtocolType.ABY);
    }

    public AbyWrapper(List<OpenHuFuRpc> rpcs, int me, List<Integer> ids, String address, int port) {
        loadAby();
        this.rpcs = rpcs;
        this.me = me;
        this.ids = ids;
        this.num = ids.size();
        this.address = address;
        this.port = port;
    }

    protected void rpcSend(long taskId, int stepId, int no, List<byte[]> data) {
        DataPacketHeader senderHeader = new DataPacketHeader(taskId, ProtocolType.ABY.getId(), stepId, ids.get(me), ids.get(no));
        rpcs.get(me).send(DataPacket.fromByteArrayList(senderHeader, data));
    }

    protected List<byte[]> rpcRecv(long taskId, int stepId, int no) {
        DataPacketHeader expectHeader = new DataPacketHeader(taskId, ProtocolType.ABY.getId(), stepId, ids.get(no), ids.get(me));
        DataPacket recv = rpcs.get(me).receive(expectHeader);
        assert(recv != null);
        return recv.getPayload(); 
    }

    public abstract Object run(long taskId, List<Integer> parties, Object input, OperatorType op) throws ProtocolException;
}
