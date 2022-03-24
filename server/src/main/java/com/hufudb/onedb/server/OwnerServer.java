package com.hufudb.onedb.server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public abstract class OwnerServer {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerServer.class);
  protected final int port;
  protected final Server server;
  protected final OwnerService service;

  public OwnerServer(ServerBuilder<?> serverBuilder, int port, OwnerService service) throws IOException {
    this.port = port;
    this.service = service;
    this.server = serverBuilder.addService(service).build();
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
          OwnerServer.this.stop();
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
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
