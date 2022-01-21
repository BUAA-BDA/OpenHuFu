package com.hufudb.onedb.backend;

import java.io.IOException;

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
public class DBServerConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(ClientConfiguration.class);

  @Value("${db.type}")
  private String type;
  @Value("${db.url}")
  private String url;
  @Value("${db.catalog}")
  private String catalog;
  @Value("${db.user}")
  private String user;
  @Value("${db.passwd}")
  private String passwd;
  @Value("${grpc.port}")
  private int port;
  @Value("${grpc.hostname}")
  private String hostname;

  @Bean
  @ConditionalOnProperty(name={"db.enable"}, havingValue = "true")
  public DBService initService() {
    switch (type) {
      case "postgresql":
        return new PostgresqlService(hostname, port, catalog, url, user, passwd);
      default:
        LOG.error("database {} is not supported", type);
        return null;
    }
  }

  @Bean
  @ConditionalOnProperty(name={"db.enable"}, havingValue = "true")
  public CommandLineRunner initPostgresqlServer(DBService service) {
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
        try {
          server.blockUntilShutdown();
        } catch (InterruptedException e) {
          LOG.warn(e.getMessage());
        }
      }
    };
  }
}
