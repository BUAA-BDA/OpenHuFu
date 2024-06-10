package group.bda.federate.driver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.rpc.FederateGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Federate server interface
 *
 */
public abstract class FederateDBServer {
  private static final Logger LOG = LogManager.getLogger(FederateDBServer.class);
  protected final int port;
  protected final Server server;

  public FederateDBServer(ServerBuilder<?> serverBuilder, int port, FederateGrpc.FederateImplBase service)
      throws IOException {
    this.port = port;
    server = serverBuilder.addService(service)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown
        // hook.
        LOG.info("*** shutting down gRPC server since JVM is shutting down");
        try {
          FederateDBServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        LOG.info("*** server shut down");
      }
    });
  }

  /** Stop serving requests and shutdown resources. */
  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon
   * threads.
   */
  protected void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
