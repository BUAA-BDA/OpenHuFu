package com.hufudb.onedb.backend;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.hufudb.onedb.OneDB;
import com.hufudb.onedb.core.data.AliasTableInfo;
import com.hufudb.onedb.core.table.TableMeta;
import com.hufudb.onedb.server.DBServer;
import com.hufudb.onedb.server.DBService;
import com.hufudb.onedb.server.postgresql.PostgresqlServer;
import com.hufudb.onedb.server.postgresql.PostgresqlService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BackendConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(BackendConfiguration.class);

  @Value("${owner.db.type}")
  private String type;
  @Value("${owner.db.url}")
  private String url;
  @Value("${owner.db.catalog}")
  private String catalog;
  @Value("${owner.db.user}")
  private String user;
  @Value("${owner.db.passwd}")
  private String passwd;
  @Value("${owner.grpc.port}")
  private int port;
  @Value("${owner.grpc.hostname}")
  private String hostname;
  @Value("${owner.schema.path}")
  private String ownerConfigPath;

  @Value("${user.endpoint:}")
  private List<String> endpoints;
  @Value("${user.schema.path:}")
  private String userConfigPath;

  private List<TableMeta> tableMetas;

  private List<AliasTableInfo> tableInfos;

  @Bean
  @ConditionalOnProperty(name={"owner.db.enable"}, havingValue = "true")
  public DBService initService() {
    try (Reader reader = Files.newBufferedReader(Paths.get(ownerConfigPath))) {
      tableInfos = new Gson().fromJson(reader, new TypeToken<ArrayList<AliasTableInfo>>() {}.getType());
    } catch (IOException|JsonSyntaxException e) {
      tableInfos = ImmutableList.of();
      LOG.warn("fail to read schema.path");
    }
    switch (type) {
      case "postgresql":
        return new PostgresqlService(hostname, port, catalog, url, user, passwd, tableInfos);
      default:
        LOG.error("database {} is not supported", type);
        return null;
    }
  }

  private void initClient(OneDB client) {
    for (String endpoint : endpoints) {
      LOG.info("add Owner {}", endpoint);
      client.addDB(endpoint);
    }
    try (Reader reader = Files.newBufferedReader(Paths.get(userConfigPath))) {
      tableMetas = new Gson().fromJson(reader, new TypeToken<ArrayList<TableMeta>>() {}.getType());
      for (TableMeta meta : tableMetas) {
        client.createOneDBTable(meta);
      }
    } catch (IOException|JsonSyntaxException e) {
      LOG.warn("fail to load global table config");
    }
  }

  @Bean
  @ConditionalOnProperty(name={"owner.db.enable"}, havingValue = "true")
  public CommandLineRunner Server(OneDB client, DBService service) {
    LOG.info("init Server");
    return args -> {
      DBServer server = null;
      switch (type) {
        case "postgresql":
          server = new PostgresqlServer(port, (PostgresqlService)service);
          break;
        default:
          LOG.error("database {} is not supported", type);
      }
      if (server != null) {
        server.start();
        initClient(client);
        try {
          server.blockUntilShutdown();
        } catch (InterruptedException e) {
          LOG.warn(e.getMessage());
        }
      }
    };
  }

  @Bean
  @ConditionalOnProperty(name={"owner.db.enable"}, havingValue = "false")
  CommandLineRunner Client(OneDB client) {
    return args -> {
      initClient(client);
    };
  }
}
