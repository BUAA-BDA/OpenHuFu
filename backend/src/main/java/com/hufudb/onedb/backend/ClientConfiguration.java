package com.hufudb.onedb.backend;

import com.hufudb.onedb.OneDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClientConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(ClientConfiguration.class);

  @Bean
  CommandLineRunner initClient(OneDB client) {
    String endpoint1 = "localhost:12345";
    String endpoint2 = "localhost:12346";
    String endpoint3 = "localhost:12347";
    return args -> {
      client.addDB(endpoint1);
      LOG.info("preloading {}", endpoint1);
      client.addDB(endpoint2);
      LOG.info("preloading {}", endpoint2);
      client.addDB(endpoint3);
      LOG.info("preloading {}", endpoint3);
    };
  }
}
