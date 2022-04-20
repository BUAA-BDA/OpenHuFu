package com.hufudb.onedb.owner;

import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OwnerServer {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerServer.class);
  protected final int port;
  protected final Server server;
  protected final OwnerService service;
  protected final ServerCredentials creds;

  public OwnerServer(int port, OwnerService service, ServerCredentials creds)
      throws IOException {
    this.port = port;
    this.service = service;
    if (creds == null) {
      this.server = ServerBuilder.forPort(port).addService(service).build();
      this.creds = null;
    } else {
      this.server = Grpc.newServerBuilderForPort(port, creds).addService(service).build();
      this.creds = creds;
    }
  }

  public static ServerCredentials generateCerd(String certChainPath, String privateKeyPath) {
    try {
      File certChainFile = new File(certChainPath);
      File privateKeyFile = new File(privateKeyPath);
      return TlsServerCredentials.create(certChainFile, privateKeyFile);
    } catch (Exception e) {
      LOG.error("Fail to read certChainFile or privateKeyFile: {}", e.getMessage());
      return null;
    }
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
      service.beforeStop();
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
