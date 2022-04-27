package com.hufudb.onedb.owner.config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DBConfig {
  public static final Logger LOG = LoggerFactory.getLogger(DBConfig.class);

  OwnerConfig generateConfig();
}
