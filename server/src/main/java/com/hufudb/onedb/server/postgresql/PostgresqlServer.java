package com.hufudb.onedb.server.postgresql;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.gson.Gson;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.grpc.ServerBuilder;
import com.hufudb.onedb.server.DBServer;

public class PostgresqlServer extends DBServer {

  public PostgresqlServer(PostgresqlConfig config) throws IOException {
    super(ServerBuilder.forPort(config.port), config.port, new PostgresqlService(config));
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
      PostgresqlServer server = new PostgresqlServer(pConfig);
      server.start();
      server.blockUntilShutdown();
    } catch (ParseException | IOException | InterruptedException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
