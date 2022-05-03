package com.hufudb.onedb.owner;

import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.PublishedTableInfo;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.translator.OneDBTranslator;
import com.hufudb.onedb.core.zk.DBZkClient;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.LeafQueryProto;
import com.hufudb.onedb.rpc.OneDBCommon.OwnerInfoProto;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import com.hufudb.onedb.rpc.grpc.concurrent.ConcurrentBuffer;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.ServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public abstract class OwnerService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerService.class);
  protected final String endpoint;
  private final Map<String, TableInfo> localTableInfoMap; // localName -> localTableInfo
  private final ReadWriteLock localLock;
  private final Map<String, PublishedTableInfo> publishedTableInfoMap; // publishedTableName
                                                                       // -> publishedTableInfo
  private final ReadWriteLock publishedLock;
  private final DBZkClient zkClient;
  protected final ExecutorService threadPool;
  protected final OneDBRpc ownerSideRpc;
  protected final ConcurrentBuffer<Long, DataSet> resultBuffer; // taskId -> bufferDataSet

  public OwnerService(String zkServers, String zkRootPath, String endpoint, String digest,
      ExecutorService threadPool, OneDBRpc ownerSideRpc) {
    this.localTableInfoMap = new HashMap<>();
    this.publishedTableInfoMap = new HashMap<>();
    this.threadPool = threadPool;
    this.localLock = new ReentrantReadWriteLock();
    this.publishedLock = new ReentrantReadWriteLock();
    this.endpoint = endpoint;
    this.ownerSideRpc = ownerSideRpc;
    this.resultBuffer = new ConcurrentBuffer<Long, DataSet>();
    if (zkServers == null || zkRootPath == null || digest == null) {
      zkClient = null;
    } else {
      DBZkClient client;
      try {
        client = new DBZkClient(zkServers, zkRootPath, endpoint, digest.getBytes());
      } catch (Exception e) {
        LOG.error("Error when init DBZkClient: {}", e.getMessage());
        client = null;
      }
      this.zkClient = client;
    }
  }

  @Override
  public void leafQuery(LeafQueryProto request, StreamObserver<DataSetProto> responseObserver) {
    Header header = OneDBContext.getOutputHeader(request);
    StreamObserverDataSet obDataSet = new StreamObserverDataSet(responseObserver, header);
    try {
      oneDBQueryInternal(request, obDataSet);
    } catch (SQLException e) {
      LOG.error("error when query table [{}]", request.getTableName());
      e.printStackTrace();
    }
    obDataSet.close();
  }

  @Override
  public void getOwnerInfo(GeneralRequest request,
      StreamObserver<OwnerInfoProto> responseObserver) {
    Party party = ownerSideRpc.ownParty();
    LOG.info("Get owner info {}", party);
    responseObserver.onNext(
        OwnerInfoProto.newBuilder().setId(party.getPartyId()).setEndpoint(endpoint).build());
    responseObserver.onCompleted();
  }

  @Override
  public void addOwner(OwnerInfoProto request, StreamObserver<GeneralResponse> responseObserver) {
    LOG.info("Connect to owner {}", OneDBOwnerInfo.fromProto(request));
    boolean ok = ownerSideRpc.addParty(OneDBOwnerInfo.fromProto(request));
    ownerSideRpc.connect();
    responseObserver.onNext(GeneralResponse.newBuilder().setStatus(ok ? 0 : 1)
        .setMsg(ok ? "" : "Fail to add owner").build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTableHeader(GeneralRequest request, StreamObserver<HeaderProto> responseObserver) {
    Header fakeHeader = getPublishedTableHeader(request.getValue());
    HeaderProto headerProto = fakeHeader.toProto();
    LOG.info("Get header of table {} {}", request.getValue(), fakeHeader);
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  public List<PublishedTableInfo> getAllPublishedTable() {
    publishedLock.readLock().lock();
    List<PublishedTableInfo> infos =
        publishedTableInfoMap.values().stream().map(vinfo -> vinfo).collect(Collectors.toList());
    publishedLock.readLock().unlock();
    return infos;
  }

  // todo: rename the funciton and rpc
  @Override
  public void getAllLocalTable(GeneralRequest request,
      StreamObserver<LocalTableListProto> responseObserver) {
    LocalTableListProto.Builder builder = LocalTableListProto.newBuilder();
    getAllPublishedTable().forEach(info -> builder.addTable(info.getFakeTableInfo().toProto()));
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table infos", builder.getTableCount());
    responseObserver.onCompleted();
  }

  protected final boolean registerTable2Zk(String schema, String globalName, String localName) {
    if (zkClient == null) {
      LOG.warn("DBZkClient is not initialized, fail to register {} to {}/{}", localName, schema,
          globalName);
      return true;
    }
    return zkClient.registerTable(schema, globalName, endpoint, localName);
  }

  protected String getOriginTableName(String publishedTableName) {
    return publishedTableInfoMap.get(publishedTableName).getOriginTableName();
  }

  protected Header getPublishedTableHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return Header.EMPTY;
    } else {
      return info.getFakeHeader();
    }
  }

  protected Header getVirtualHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      return Header.EMPTY;
    } else {
      return info.getVirtualHeader();
    }
  }

  public final void addLocalTableInfo(TableInfo tableInfo) {
    localLock.writeLock().lock();
    LOG.info("Add Local Table {}", tableInfo);
    localTableInfoMap.put(tableInfo.getName(), tableInfo);
    localLock.writeLock().unlock();
  }

  public final TableInfo getLocalTableInfo(String tableName) {
    localLock.readLock().lock();
    TableInfo info = localTableInfoMap.get(tableName);
    localLock.readLock().unlock();
    return info;
  }

  public final List<TableInfo> getAllFakeTable() {
    localLock.readLock().lock();
    List<TableInfo> results = publishedTableInfoMap.values().stream()
        .map(info -> info.getFakeTableInfo()).collect(Collectors.toList());
    localLock.readLock().unlock();
    return results;
  }

  public final List<TableInfo> getAllLocalTable() {
    List<TableInfo> infos = new ArrayList<>();
    localLock.readLock().lock();
    for (TableInfo info : localTableInfoMap.values()) {
      infos.add(info);
    }
    localLock.readLock().unlock();
    return infos;
  }

  public void clearPublishedTable() {
    publishedLock.writeLock().lock();
    publishedTableInfoMap.clear();
    publishedLock.writeLock().unlock();
  }

  public void dropPublishedTable(String tableName) {
    publishedLock.writeLock().lock();
    publishedTableInfoMap.remove(tableName);
    publishedLock.writeLock().unlock();
  }

  public void initPublishedTable(List<POJOPublishedTableInfo> infos) {
    if (infos == null)
      return;
    for (POJOPublishedTableInfo info : infos) {
      addPublishedTable(info);
    }
  }

  public PublishedTableInfo generatePublishedTableInfo(POJOPublishedTableInfo publishedTableInfo) {
    List<Field> pFields = new ArrayList<>();
    List<Integer> mappings = new ArrayList<>();
    TableInfo originInfo = localTableInfoMap.get(publishedTableInfo.getOriginTableName());
    List<Field> publishedFields = publishedTableInfo.getPublishedFields();
    List<Integer> originNames = publishedTableInfo.getOriginColumns();
    for (int i = 0; i < publishedFields.size(); ++i) {
      if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
        pFields.add(publishedFields.get(i));
        mappings.add(originNames.get(i));
      }
    }
    return new PublishedTableInfo(originInfo, publishedTableInfo.getPublishedTableName(), pFields,
        mappings);
  }

  public boolean addPublishedTable(POJOPublishedTableInfo publishedTableInfo) {
    return addPublishedTable(generatePublishedTableInfo(publishedTableInfo));
  }

  public boolean addPublishedTable(PublishedTableInfo publishedTable) {
    publishedLock.writeLock().lock();
    if (publishedTableInfoMap.containsKey(publishedTable.getPublishedTableName())) {
      LOG.error("published table {} already exist", publishedTable.getPublishedTableName());
      publishedLock.writeLock().unlock();
      return false;
    }
    LOG.info("Add Published Table {}", publishedTable);
    publishedTableInfoMap.put(publishedTable.getPublishedTableName(), publishedTable);
    publishedLock.writeLock().unlock();
    return true;
  }

  public boolean changeCatalog(String catalog) {
    LOG.error("change catalog operation is not supported in your database");
    return false;
  }

  // template function for SQL database, rewrite this for database without sql
  protected void oneDBQueryInternal(LeafQueryProto query, DataSet dataSet) throws SQLException {
    String sql = generateSQL(query);
    if (sql.isEmpty()) {
      return;
    }
    executeSQL(sql, dataSet);
  }

  protected String generateSQL(LeafQueryProto query) {
    String originTableName = getOriginTableName(query.getTableName());
    Header tableHeader = getPublishedTableHeader(query.getTableName());
    LOG.info("{}: {}", originTableName, tableHeader);
    final List<String> filters = OneDBTranslator.tranlateExps(tableHeader,
        OneDBExpression.fromProto(query.getWhereExpList()));
    final List<String> selects = OneDBTranslator.tranlateExps(tableHeader,
        OneDBExpression.fromProto(query.getSelectExpList()));
    final List<String> groups =
        query.getGroupList().stream().map(ref -> selects.get(ref)).collect(Collectors.toList());
    // order by
    List<String> order = query.getOrderList();
    StringBuilder orderClause = new StringBuilder();
    if (!order.isEmpty()) {
      for (int i = 0; i < order.size(); i++) {
        String[] tmp = order.get(i).split(" ");
        orderClause.append(selects.get(Integer.parseInt(tmp[0]))).append(" ").append(tmp[1]);
        if (i != order.size() - 1) {
          orderClause.append(" , ");
        }
      }
    }
    StringBuilder sql = new StringBuilder();
    // select from clause
    if (query.getAggExpCount() > 0) {
      final List<String> aggs =
          OneDBTranslator.translateAgg(selects, OneDBExpression.fromProto(query.getAggExpList()));
      sql.append(String.format("SELECT %s from %s", String.join(",", aggs), originTableName));
    } else {
      sql.append(String.format("SELECT %s from %s", String.join(",", selects), originTableName));
    }
    // where clause
    if (!filters.isEmpty()) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    if (!groups.isEmpty()) {
      sql.append(String.format(" group by %s", String.join(",", groups)));
    }
    if (orderClause.length() > 0) {
      sql.append(" ORDER BY ");
    }
    sql.append(orderClause);
    if (query.getFetch() != 0) {
      sql.append(" LIMIT ").append(query.getFetch() + query.getOffset());
    }
    LOG.info(sql.toString());
    return sql.toString();
  }

  protected void fillDataSet(ResultSet rs, DataSet dataSet) throws SQLException {
    final Header header = dataSet.getHeader();
    final int columnSize = header.size();
    while (rs.next()) {
      Row.RowBuilder builder = Row.newBuilder(columnSize);
      for (int i = 0; i < columnSize; ++i) {
        if (header.getLevel(i).equals(Level.HIDDEN)) {
          continue;
        }
        switch (header.getType(i)) {
          case BYTE:
            builder.set(i, rs.getByte(i + 1));
            break;
          case SHORT:
            builder.set(i, rs.getShort(i + 1));
            break;
          case INT:
            builder.set(i, rs.getInt(i + 1));
            break;
          case LONG:
            builder.set(i, rs.getLong(i + 1));
            break;
          case FLOAT:
            builder.set(i, rs.getFloat(i + 1));
            break;
          case DOUBLE:
            builder.set(i, rs.getDouble(i + 1));
            break;
          case STRING:
            builder.set(i, rs.getString(i + 1));
            break;
          case BOOLEAN:
            builder.set(i, rs.getBoolean(i + 1));
            break;
          case DATE:
            // Divide by 86400000L to prune time field
            Long date = rs.getDate(i + 1).getTime() / 86400000L;
            builder.set(i, date);
            break;
          case TIME:
            Long time = rs.getTime(i + 1).getTime();
            builder.set(i, time);
            break;
          case TIMESTAMP:
            Long timeStamp = rs.getTimestamp(i + 1).getTime();
            builder.set(i, timeStamp);
            break;
          default:
            builder.set(i, rs.getObject(i + 1));
            break;
        }
      }
      dataSet.addRow(builder.build());
    }
  }

  protected void executeSQL(String sql, DataSet dataSet) throws SQLException {
    ResultSet rs = null;
    try {
      rs = getStatement().executeQuery(sql);
      fillDataSet(rs, dataSet);
      LOG.info("Execute {} returned {} rows", sql, dataSet.getRowCount());
    } catch (SQLException e) {
      LOG.error("Fail to execute SQL [{}]: {}", sql, e.getMessage());
    } finally {
      rs.close();
    }
  }

  protected Statement getStatement() {
    throw new UnsupportedOperationException("not support getStatement");
  }

  // must call this function in subclass's constructor
  public abstract void loadAllTableInfo();

  protected abstract TableInfo loadTableInfo(String tableName);

  protected abstract void beforeStop();
}
