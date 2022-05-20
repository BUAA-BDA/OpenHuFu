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
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.OwnerConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for owner and user-client see @OwnerConfigFile.java for detail
 */
@Configuration
public class BackendConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(BackendConfiguration.class);

  @Value("${owner.id}")
  private int id;

  @Value("${owner.port}")
  private int port;

  @Value("${owner.threadnum}")
  private int threadnum;

  @Value("${owner.hostname}")
  private String hostname;

  @Value("${owner.privatekeypath}")
  private String privatekeypath;

  @Value("${owner.certchainpath}")
  private String certchainpath;

  @Value("${owner.trustcertpath}")
  private String trustcertpath;

  @Value("${owner.schema.path}")
  private String ownerSchemaConfigPath;

  @Value("${owner.adapter.type}")
  private String type;

  @Value("${owner.adapter.url}")
  private String url;

  @Value("${owner.adapter.catalog}")
  private String catalog;

  @Value("${owner.adapter.user}")
  private String user;

  @Value("${owner.adapter.passwd}")
  private String passwd;

  @Value("${user.endpoint}")
  private List<String> endpoints;

  @Value("${user.trustcertpath}")
  private List<String> trustcertpaths;

  @Value("${user.schema.path}")
  private String userSchemaConfigPath;

  private List<GlobalTableConfig> userTableConfig;

  OwnerConfig generateOwnerConfig() {
    OwnerConfigFile ownerConfigFile = new OwnerConfigFile(id, port, threadnum, hostname,
        privatekeypath, certchainpath, trustcertpath);
    List<PojoPublishedTableSchema> ownerTableConfig = ImmutableList.of();
    try (Reader reader = Files.newBufferedReader(Paths.get(ownerSchemaConfigPath))) {
      ownerTableConfig = new Gson().fromJson(reader,
          new TypeToken<ArrayList<PojoPublishedTableSchema>>() {}.getType());
    } catch (IOException | JsonSyntaxException e) {
      LOG.warn("fail to read schema.path");
    }
    ownerConfigFile.tables = ownerTableConfig;
    AdapterConfig adapterConfig = new AdapterConfig(type);
    adapterConfig.catalog = catalog;
    adapterConfig.url = url;
    adapterConfig.user = user;
    adapterConfig.passwd = passwd;
    ownerConfigFile.adapterconfig = adapterConfig;
    return ownerConfigFile.generateConfig();
  }

  private void initClient(OneDB client) {
    for (String endpoint : endpoints) {
      LOG.info("add Owner {}", endpoint);
      client.addOwner(endpoint);
    }
    try (Reader reader = Files.newBufferedReader(Paths.get(userSchemaConfigPath))) {
      userTableConfig =
          new Gson().fromJson(reader, new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
      for (GlobalTableConfig config : userTableConfig) {
        client.createOneDBTable(config);
      }
    } catch (IOException | JsonSyntaxException e) {
      LOG.warn("fail to load global table config");
    }
  }

  @Bean
  @ConditionalOnProperty(name = {"owner.db.enable"}, havingValue = "true")
  public CommandLineRunner Server(OneDB client, OwnerService service) {
    LOG.info("init Server");
    return args -> {
      OwnerServer server = new OwnerServer(generateOwnerConfig());
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
  @ConditionalOnProperty(name = {"owner.db.enable"}, havingValue = "false")
  CommandLineRunner Client(OneDB client) {
    return args -> {
      initClient(client);
    };
  }
}
