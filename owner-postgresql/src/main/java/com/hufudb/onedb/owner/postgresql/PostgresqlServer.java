package com.hufudb.onedb.owner.postgresql;

import com.google.gson.Gson;
import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.owner.OwnerServer;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import io.grpc.ServerCredentials;

public class PostgresqlServer extends OwnerServer {

  public PostgresqlServer(PostgresqlConfig config, ExecutorService threadPool, ServerCredentials certs) throws IOException {
    super(config.port, new PostgresqlService(config, threadPool), threadPool, certs);
  }

  public static void main(String[] args) {
    Options options = new Options();
    Option config = new Option("c", "config", true, "postgresql config");
    config.setRequired(true);
    options.addOption(config);
    CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      PostgresqlConfig pConfig = gson.fromJson(reader, PostgresqlConfig.class);
      ServerCredentials creds = OwnerServer.generateCerd(pConfig.certchainpath, pConfig.privatekeypath);
      int threadNum = pConfig.threadnum == 0 ? OneDBConfig.SERVER_THREAD_NUM : pConfig.threadnum;
      ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
      PostgresqlServer server = new PostgresqlServer(pConfig, threadPool, creds);
      server.start();
      server.blockUntilShutdown();
    } catch (ParseException | IOException | InterruptedException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
