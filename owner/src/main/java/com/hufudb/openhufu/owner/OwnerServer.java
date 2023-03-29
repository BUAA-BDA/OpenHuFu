package com.hufudb.openhufu.owner;


import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import com.hufudb.openhufu.owner.config.OwnerConfig;
import com.hufudb.openhufu.owner.config.OwnerConfigFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerServer {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerServer.class);
  protected final String hostname;
  protected final int port;
  protected final Server server;
  protected final OwnerService service;
  protected final ServerCredentials creds;
  protected final ExecutorService threadPool;

  public OwnerServer(OwnerConfig config) throws IOException {
    this.hostname = config.hostname;
    this.port = config.port;
    this.threadPool = config.threadPool;
    BindableService pipeService = config.acrossOwnerRpc.getgRpcService();
    this.service = new OwnerService(config);
    if (config.useTLS) {
      this.creds = config.serverCerts;
      this.server = Grpc.newServerBuilderForPort(port, creds).addService(service)
          .addService(pipeService).build();
      LOG.info("Owner Server {} start with TLS", config.party.getPartyId());
    } else {
      this.creds = null;
      this.server = ServerBuilder.forPort(port).addService(service).addService(pipeService).build();
      LOG.info("Owner Server {} start in plaintext", config.party.getPartyId());
    }
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Server started, listening on {}", port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("*** shutting down gRPC server since JVM is shutting down");
        try {
          OwnerServer.this.stop();
        } catch (InterruptedException e) { // NOSONAR
          LOG.error("error when stop server", e);
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

  public OwnerService getService() {
    return service;
  }

  public String getEndpoint() {
    return String.format("%s:%d", hostname, port);
  }

  public static OwnerServer create(String configPath) throws IOException {
    Gson gson = new Gson();
    Reader reader = Files.newBufferedReader(Paths.get(configPath));
    OwnerConfigFile config = gson.fromJson(reader, OwnerConfigFile.class);
    return new OwnerServer(config.generateConfig());
  }

  public static void main(String[] args) {
    Options options = new Options();
    Option cmdConfig = new Option("c", "config", true, "owner config file path");
    cmdConfig.setRequired(true);
    options.addOption(cmdConfig);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      OwnerServer server = create(cmd.getOptionValue("config"));
      server.start();
      server.blockUntilShutdown();
    } catch (Exception e) { // NOSONAR
      LOG.error("Error when start owner server", e);
      System.exit(1);
    }
  }

}
