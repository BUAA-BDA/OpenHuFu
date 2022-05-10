package com.hufudb.onedb.owner;

import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.OwnerConfigFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerServer {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerServer.class);
  protected final int port;
  protected final Server server;
  protected final OwnerService service;
  protected final ServerCredentials creds;
  protected final ExecutorService threadPool;

  public OwnerServer(OwnerConfig config) throws IOException {
    this.port = config.port;
    this.threadPool = config.threadPool;
    BindableService pipeService = config.acrossOwnerRpc.getgRpcService();
    this.service = new OwnerService(config);
    if (config.useTLS) {
      this.creds = config.serverCerts;
      this.server = Grpc.newServerBuilderForPort(port, creds).addService(service)
          .addService(pipeService).build();
    } else {
      this.creds = null;
      this.server = ServerBuilder.forPort(port).addService(service).addService(pipeService).build();
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
      service.shutdown();
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) {
    Options options = new Options();
    Option cmdConfig = new Option("c", "config", true, "postgresql config");
    cmdConfig.setRequired(true);
    options.addOption(cmdConfig);
    CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      OwnerConfigFile config = gson.fromJson(reader, OwnerConfigFile.class);
      OwnerServer server = new OwnerServer(config.generateConfig());
      server.start();
      server.blockUntilShutdown();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
