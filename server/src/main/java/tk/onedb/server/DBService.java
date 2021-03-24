package tk.onedb.server;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import tk.onedb.OneDBService.Query;
import tk.onedb.ServiceGrpc;
import tk.onedb.rpc.OneDBCommon.DataSetProto;
import tk.onedb.server.data.TableInfo;

public class DBService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DBService.class);
  protected final Map<String, TableInfo> tableInfoMap;

  protected DBService() {
    this.tableInfoMap = new HashMap<>();
  }

  @Override
  public void oneDBQuery(Query request, StreamObserver<DataSetProto> responseObserver) {
    
  }
}
