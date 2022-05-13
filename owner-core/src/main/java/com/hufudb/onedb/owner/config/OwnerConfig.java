package com.hufudb.onedb.owner.config;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import io.grpc.ChannelCredentials;
import io.grpc.ServerCredentials;

public class OwnerConfig {
  public Party party;
  public int port;
  public String hostname;
  public ExecutorService threadPool;
  public Adapter adapter;
  public OneDBRpc acrossOwnerRpc;
  public boolean useTLS;
  public ServerCredentials serverCerts;
  public ChannelCredentials clientCerts;
  public List<PojoPublishedTableSchema> tables;
}
