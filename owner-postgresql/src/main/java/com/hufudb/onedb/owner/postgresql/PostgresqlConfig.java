package com.hufudb.onedb.owner.postgresql;

import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.TemplateConfig;

public class PostgresqlConfig extends TemplateConfig {
  @Override
  public OwnerConfig generateConfig() {
    OwnerConfig config = generateConfigInternal();
    config.userOwnerService = new PostgresqlService(hostname, port, catalog, url, user, passwd,
        tables, config.threadPool, config.acrossOwnerService);
    return config;
  }
}
