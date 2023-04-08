package com.hufudb.openhufu.mpc.lib.aby;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpcManager;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;

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

    private List<OpenHuFuRpc> rpcs = new ArrayList<>();
    private List<MultiCmp> cmps = new ArrayList<>();
    private List<Integer> ids = new ArrayList<>();
    private int partyNum = 10;

    @Before
    public void setUp() throws IOException {
        String[] ownerNames = new String[partyNum];
        List<Party> parties = new ArrayList<>();
        List<Channel> channels = new ArrayList<>();
        for (int p = 0; p < partyNum; p++) {
            ids.add(2 * p + 1);
            ownerNames[p] = InProcessServerBuilder.generateName();
            parties.add(new OpenHuFuOwnerInfo(ids.get(p), ownerNames[p]));
            channels.add(grpcCleanup.register(
                Grpc.newChannelBuilder("127.0.0.1:" + (7000 + p), InsecureChannelCredentials.create()).build()
            ));
        }
        OpenHuFuRpcManager manager = new OpenHuFuRpcManager(parties, channels);
        for (int p = 0; p < partyNum; p++) {
            rpcs.add((OpenHuFuRpc) manager.getRpc(ids.get(p)));
            Server s = Grpc.newServerBuilderForPort(7000 + p, InsecureServerCredentials.create()).
                directExecutor().addService(rpcs.get(p).getgRpcService()).build();
            s.start();
            
            grpcCleanup.register(s);
            rpcs.get(p).connect();
        }
        for (int p = 0; p < partyNum; p++) {
            MultiCmp c = new MultiCmp(rpcs, p, ids, "127.0.0.1", 8000);
            cmps.add(c);
        }

        for (int p = 0; p < partyNum; p++) {
            LOG.info("rpcs[{}]: {}, {}, {}", p, ownerNames[p], rpcs.get(p).ownParty().getPartyId(), channels.get(p));
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
        inputs[2] = 229;
        inputs[3] = 129;

        ArrayList<Future<Integer>> results = new ArrayList<>();
        for (int p = 0; p < partyNum; p++) {
            MultiCmp cmp = cmps.get(p);
            int input = inputs[p];
            Future<Integer> res = service.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    LOG.info("input = {}", input);
                    int ret = (int)cmp.run(taskId, ids, input, OperatorType.MAX);
                    return ret;
                }
            });
            results.add(res);
        }
        for (int p = 0; p < partyNum; p++) {
            int res = (int)(results.get(p).get());
            if (p == 0) {
                assertEquals(res, 229);
            }
        }
    }
    
    @Test
    public void testMultiCmpMultiTimes() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(partyNum);

        int[] inputs = new int[partyNum];

        for (int taskId = 0; taskId < 50; taskId++) {
            int ans = 0;
            for (int i = 0; i < partyNum; i++) {
                inputs[i] = (i * 10 + taskId) % (partyNum + 7);
                if (inputs[i] > ans) {
                    ans = inputs[i];
                }
            }
            ArrayList<Future<Integer>> results = new ArrayList<>();
            for (int p = 0; p < partyNum; p++) {
                MultiCmp cmp = cmps.get(p);
                int input = inputs[p];
                long tid = taskId;
                Future<Integer> res = service.submit(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        LOG.info("cmp input = {}", input);
                        int ret = (int)cmp.run(tid, ids, input, OperatorType.MAX);
                        return ret;
                    }
                });
                results.add(res);
            }
            for (int p = 0; p < partyNum; p++) {
                int res = (int)(results.get(p).get());
                if (p == 0) {
                    LOG.info("taskId = {}", taskId);
                    assertEquals(res, ans);
                }
            }
        }
        
    }
}
