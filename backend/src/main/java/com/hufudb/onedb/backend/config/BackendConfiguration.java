package com.hufudb.onedb.backend.config;

import java.io.IOException;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.utils.PojoOwnerInfo;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.user.OneDB;
import com.hufudb.onedb.user.utils.ModelGenerator;
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

  @Value("${owner.config:@null}")
  private String ownerConfigPath;

  @Value("${user.config:@null}")
  private String userConfigPath;

  @Bean
  public OneDB initUser() {
    return new OneDB();
  }

  @Bean
  @ConditionalOnProperty(name = {"owner.enable"}, havingValue = "true")
  public OwnerServer initOwner() {
    try {
      return OwnerServer.create(ownerConfigPath);
    } catch (IOException e) {
      LOG.error("Fail to init owner side server");
      e.printStackTrace();
      return null;
    }
  }

  private void initClient(OneDB client) {
    ModelGenerator.Model model = null;
    try {
      model = ModelGenerator.parseModel(userConfigPath);
    } catch (IOException e) {
      LOG.error("Fail to parse user config file {}: {}", userConfigPath, e.getMessage());
      return;
    }
    for (PojoOwnerInfo info : model.owners) {
      if (info.trustCertPath == null) {
        client.addOwner(info.endpoint);
      } else {
        client.addOwner(info.endpoint, info.trustCertPath);
      }
    }
    for (GlobalTableConfig table : model.tables) {
      client.createOneDBTable(table);
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
