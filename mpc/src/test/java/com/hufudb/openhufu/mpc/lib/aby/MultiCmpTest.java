package com.hufudb.openhufu.mpc.lib.aby;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpcManager;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import com.hufudb.openhufu.mpc.lib.LibraryLoader;
import io.grpc.Channel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.hufudb.openhufu.mpc.ProtocolFactory;
import com.hufudb.openhufu.mpc.ProtocolType;

import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiCmpTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private static final Logger LOG = LoggerFactory.getLogger(MultiCmpTest.class);

    private ArrayList<OpenHuFuRpc> rpcs = new ArrayList<>();
    private ArrayList<MultiCmp> cmps = new ArrayList<>();
    private int partyNum = 2;

    @Before
    public void setUp() throws IOException {
        String openhufuRoot = System.getenv("OPENHUFU_ROOT");
        Path libDir = Paths.get(openhufuRoot, "lib");
        Map<ProtocolType, ProtocolFactory> factories =
            LibraryLoader.loadProtocolLibrary(libDir.toString());
        ProtocolFactory factory = factories.get(ProtocolType.ABY);
        String[] ownerNames = new String[partyNum];
        List<Party> parties = new ArrayList<>();
        List<Channel> channels = new ArrayList<>();
        for (int p = 0; p < partyNum; p++) {
            ownerNames[p] = InProcessServerBuilder.generateName();
            parties.add(new OpenHuFuOwnerInfo(p, ownerNames[p]));
            channels.add(grpcCleanup.register(InProcessChannelBuilder.forName(ownerNames[p]).directExecutor().build()));
        }
        OpenHuFuRpcManager manager = new OpenHuFuRpcManager(parties, channels);
        for (int p = 0; p < partyNum; p++) {
            rpcs.add((OpenHuFuRpc) manager.getRpc(p));
            grpcCleanup.register(InProcessServerBuilder.forName(ownerNames[p]).directExecutor()
                .addService(rpcs.get(p).getgRpcService()).build().start());
            rpcs.get(p).connect();
        }
        for (int p = 0; p < partyNum; p++) {
            MultiCmp c = new MultiCmp(rpcs.get(p), p, partyNum, "127.0.0.1", 6000 + p, factory);
            cmps.add(c);
        }
    }

    @After
    public void shutdown() {
        for (OpenHuFuRpc r : rpcs) {
            r.disconnect();
        }
    }

    @Test
    public void testMultiCmp() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(partyNum);
        int taskId = 0;

        int[] inputs = new int[partyNum];
        inputs[0] = 30;
        inputs[1] = 29;

        ArrayList<Future<Integer>> results = new ArrayList<>();
        for (int p = 0; p < partyNum; p++) {
            MultiCmp cmp = cmps.get(p);
            int input = inputs[p];
            Future<Integer> res = service.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    LOG.info("input = {}", input);
                    int ret = cmp.runMultiCmp(taskId, input);
                    return ret;
                }
            });
            results.add(res);
        }
        for (int p = 0; p < partyNum; p++) {
            int res = (int)(results.get(0).get());
            if (p == 0) {
                assertEquals(res, 30);
            }
        }
    }
}
