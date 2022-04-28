package com.hufudb.onedb.owner.config;

import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import io.grpc.ChannelCredentials;
import io.grpc.ServerCredentials;

public class OwnerConfig {
  public Party party;
  public int port;
  public String hostname;
  public ExecutorService threadPool;
  public OwnerService userOwnerService;
  public OneDBRpc acrossOwnerService;
  public boolean useTLS;
  public ServerCredentials serverCerts;
  public ChannelCredentials clientCerts;
}
