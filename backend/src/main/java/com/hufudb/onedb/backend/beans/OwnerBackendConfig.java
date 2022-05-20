package com.hufudb.onedb.backend.beans;

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import io.grpc.ChannelCredentials;
import io.grpc.ServerCredentials;

public class OwnerBackendConfig extends OwnerConfig {
  public OwnerBackendConfig() {}

  public OwnerBackendConfig(Party party, int port, String hostname, ExecutorService threadPool,
      Adapter adapter, OneDBRpc acrossOwnerRpc, boolean useTLS, ServerCredentials serverCerts,
      ChannelCredentials clientCerts, List<PojoPublishedTableSchema> tables) {
    this.party = party;
    this.port = port;
    this.hostname = hostname;
    this.threadPool = threadPool;
    this.adapter = adapter;
    this.acrossOwnerRpc = acrossOwnerRpc;
    this.useTLS = useTLS;
    this.serverCerts = serverCerts;
    this.clientCerts = clientCerts;
    this.tables = tables;
  }
}
