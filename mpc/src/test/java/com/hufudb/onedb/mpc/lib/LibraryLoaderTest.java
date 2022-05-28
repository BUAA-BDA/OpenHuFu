package com.hufudb.onedb.mpc.lib;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolFactory;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.proto.OneDBService.OwnerInfo;
import org.junit.Ignore;
import org.junit.Test;

public class LibraryLoaderTest {
    static Random random = new Random();

    @Ignore
    @Test
    public void testLoadProtocolLibrary() throws InterruptedException, ExecutionException {
        String onedbRoot = System.getenv("ONEDB_ROOT");
        Path libDir = Paths.get(onedbRoot, "lib");
        System.err.println(libDir.toString());
        ProtocolFactory factory =
                LibraryLoader.loadProtocolLibrary(libDir.toString(), ProtocolType.ABY);
        ProtocolExecutor aby0 = factory.create(
                OwnerInfo.newBuilder().setEndpoint("127.0.0.1:7766").setId(0).build(),
                ProtocolType.ABY);
        assertNotNull(aby0);
        ProtocolExecutor aby1 = factory.create(
                OwnerInfo.newBuilder().setEndpoint("127.0.0.1:7766").setId(1).build(),
                ProtocolType.ABY);
        assertNotNull(aby1);
        ExecutorService service = Executors.newFixedThreadPool(2);
        final int A = Math.abs(random.nextInt());
        final int B = Math.abs(random.nextInt());
        final boolean expect = A <= B;
        Future<Boolean> r0 = service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return OneDBCodec.decodeBoolean(aby0
                        .run(0, ImmutableList.of(0, 1), ImmutableList.of(OneDBCodec.encodeInt(A)))
                        .get(0));
            }
        });
        Future<Boolean> r1 = service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return OneDBCodec.decodeBoolean(aby1
                        .run(0, ImmutableList.of(0, 1), ImmutableList.of(OneDBCodec.encodeInt(B)))
                        .get(0));
            }
        });
        if (expect) {
            assertFalse(r0.get());
            assertTrue(r1.get());
        } else {
            assertTrue(r0.get());
            assertFalse(r1.get());
        }
    }
}
