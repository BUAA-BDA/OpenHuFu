package com.hufudb.onedb.owner.mysql;

import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.TemplateConfig;

public class MysqlConfig extends TemplateConfig {
  @Override
  public OwnerConfig generateConfig() {
    OwnerConfig config = generateConfigInternal();
    config.userOwnerService = new MysqlService(hostname, port, catalog, url, user, passwd, tables,
        config.threadPool, config.acrossOwnerService);
    return config;
  }
}
