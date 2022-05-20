package com.hufudb.onedb.backend.beans;

import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.config.OwnerConfigFile;

public class OwnerBackendConfigFile extends OwnerConfigFile {

  public OwnerBackendConfigFile() {}

  public OwnerBackendConfigFile(Integer id, Integer port, Integer threadnum, String hostname,
      String privatekeypath, String certchainpath, String trustcertpath) {
    super(id, port, threadnum, hostname, privatekeypath, certchainpath, trustcertpath);
  }

  public OwnerBackendConfig generate() {
    OwnerConfig config = generateConfig();
    return new OwnerBackendConfig(config.party, config.port, config.hostname, config.threadPool, config.adapter, config.acrossOwnerRpc, config.useTLS, config.serverCerts, config.clientCerts, config.tables);
  }
}
