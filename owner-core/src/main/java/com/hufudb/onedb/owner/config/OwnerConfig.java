package com.hufudb.onedb.owner.config;

import java.io.File;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;

public class OwnerConfig {
  public Party party;
  public int port;
  public String hostname;
  public ExecutorService threadPool;
  public OwnerService userOwnerService;
  public OneDBRpc acrossOwnerService;
  public boolean useTLS;
  public File certChain;
  public File privateKey;
  public File rootCert;
}
