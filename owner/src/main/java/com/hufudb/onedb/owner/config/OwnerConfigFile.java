package com.hufudb.onedb.owner.config;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterConfig;
import com.hufudb.onedb.owner.adapter.AdapterFactory;
import com.hufudb.onedb.owner.adapter.AdapterLoader;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerConfigFile {
  private static final int THREAD_NUM = 8;
  public static final Logger LOG = LoggerFactory.getLogger(OwnerConfigFile.class);

  public int id;
  public int port;
  public int threadnum;
  public String hostname;
  public String privatekeypath;
  public String certchainpath;
  public String trustcertpath;
  public List<PojoPublishedTableSchema> tables;
  public AdapterConfig adapterconfig;

  public Adapter getAdapter() {
    Path adapterDir = Paths.get(System.getenv("ONEDB_ROOT"), "adapter");
    Map<String, AdapterFactory> adapterFactories = AdapterLoader.loadAdapters(adapterDir.toString());
    AdapterFactory factory = adapterFactories.get(adapterconfig.datasource);
    if (factory == null) {
      LOG.error("Fail to get adapter for datasource [{}]", adapterconfig.datasource);
      throw new RuntimeException("Fail to get adapter for datasource");
    }
    return factory.create(adapterconfig);
  }

  public OwnerConfig generateConfig() {
    OwnerConfig config = new OwnerConfig();
    config.party = new OneDBOwnerInfo(id, String.format("%s:%d", hostname, port));
    config.port = port;
    config.hostname = hostname;
    if (threadnum > 0) {
      config.threadPool = Executors.newFixedThreadPool(threadnum);
    } else {
      config.threadPool = Executors.newFixedThreadPool(THREAD_NUM);
    }
    if (privatekeypath != null && certchainpath != null) {
      try {
        File certChain = new File(certchainpath);
        File privateKey = new File(privatekeypath);
        config.serverCerts = TlsServerCredentials.create(certChain, privateKey);
        config.useTLS = true;
        LOG.info("load certChainFile and privateKeyFile");
      } catch (Exception e) {
        LOG.error("Fail to read certChainFile or privateKeyFile: {}", e.getMessage());
        config.useTLS = false;
      }
    }
    if (trustcertpath != null) {
      try {
        File rootCert = new File(trustcertpath);
        config.clientCerts = TlsChannelCredentials.newBuilder().trustManager(rootCert).build();
        config.acrossOwnerRpc =
            new OneDBRpc(config.party, config.threadPool, config.clientCerts);
        LOG.info("load trustcertFile");
      } catch (Exception e) {
        LOG.error("Fail to read trustcertFile: {}", e.getMessage());
        config.acrossOwnerRpc = new OneDBRpc(config.party, config.threadPool);
      }
    } else {
      config.acrossOwnerRpc = new OneDBRpc(config.party, config.threadPool);
    }
    config.adapter = getAdapter();
    config.tables = tables;
    return config;
  }
}
