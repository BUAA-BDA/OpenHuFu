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
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.OwnerConfigFile;
import com.hufudb.onedb.user.OneDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for owner and user see @OwnerConfigFile.java for detail
 */
@Configuration
public class BackendConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(BackendConfiguration.class);

  @Value("${owner.id:0}")
  private int id;

  @Value("${owner.port:0}")
  private int port;

  @Value("${owner.threadnum:4}")
  private int threadnum;

  @Value("${owner.hostname:localhost}")
  private String hostname;

  @Value("${owner.privatekeypath:@null}")
  private String privatekeypath;

  @Value("${owner.certchainpath:@null}")
  private String certchainpath;

  @Value("${owner.trustcertpath:@null}")
  private String trustcertpath;

  @Value("${owner.schema.path:@null}")
  private String ownerSchemaConfigPath;

  @Value("${owner.adapter.type:@null}")
  private String type;

  @Value("${owner.adapter.url:@null}")
  private String url;

  @Value("${owner.adapter.catalog:@null}")
  private String catalog;

  @Value("${owner.adapter.user:@null}")
  private String user;

  @Value("${owner.adapter.passwd:@null}")
  private String passwd;

  @Value("${user.endpoints:@null}")
  private List<String> endpoints;

  @Value("${user.trustcertpaths:@null}")
  private List<String> trustcertpaths;

  @Value("${user.schema.path:@null}")
  private String userSchemaConfigPath;

  private List<GlobalTableConfig> userTableConfig;

  @Bean
  @ConditionalOnProperty(name = {"owner.enable"}, havingValue = "true")
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

  @Bean
  public OneDB initUser() {
    return new OneDB();
  }

  @Bean
  @ConditionalOnProperty(name = {"owner.enable"}, havingValue = "true")
  public OwnerServer initOwner() {
    try {
      return new OwnerServer(generateOwnerConfig());
    } catch (IOException e) {
      LOG.error("Fail to init owner side server");
      e.printStackTrace();
      return null;
    }
  }

  private void initClient(OneDB client) {
    for (int i = 0; i < endpoints.size(); ++i) {
      if (trustcertpaths == null || trustcertpaths.size() < i) {
        client.addOwner(endpoints.get(i));
      } else {
        client.addOwner(endpoints.get(i), trustcertpaths.get(i));
      }
    }
    try (Reader reader = Files.newBufferedReader(Paths.get(userSchemaConfigPath))) {
      userTableConfig =
          new Gson().fromJson(reader, new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
      for (GlobalTableConfig config : userTableConfig) {
        client.createOneDBTable(config);
      }
    } catch (IOException | JsonSyntaxException e) {
      LOG.warn("fail to load global table config");
      e.printStackTrace();
    }
  }

  @Bean
  @ConditionalOnProperty(name = {"owner.enable"}, havingValue = "false")
  CommandLineRunner Client(OneDB client) {
    return args -> {
      initClient(client);
    };
  }

  @Bean
  @ConditionalOnProperty(name = {"owner.enable"}, havingValue = "true")
  public CommandLineRunner Server(OneDB client, OwnerServer server) {
    LOG.info("init Owner");
    return args -> {
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
}
