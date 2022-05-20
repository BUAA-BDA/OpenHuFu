package com.hufudb.onedb.backend.beans;

import java.io.IOException;
import com.hufudb.onedb.owner.OwnerServer;

public class OwnerBackendServer extends OwnerServer {

  public OwnerBackendServer(OwnerBackendConfig config) throws IOException {
    super(config);
  }
}
