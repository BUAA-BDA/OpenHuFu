package com.hufudb.onedb.rpc.grpc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.grpc.concurrent.ConcurrentBuffer;
import com.hufudb.onedb.rpc.grpc.pipe.PipeClient;
import com.hufudb.onedb.rpc.grpc.pipe.PipeService;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBRpc implements Rpc {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBRpc.class);
  private static int THREAD_NUM = 8;
  final Party own;
  final Set<Party> parties;
  final Map<Integer, Party> participantMap;
  final Map<Integer, PipeClient> clientMap;
  final Map<Integer, ConcurrentBuffer<DataPacketHeader, DataPacket>> bufferMap;
  final PipeService gRpcService;
  final ExecutorService threadPool;
  long payloadByteLength;
  long dataPacketNum;
  final ChannelCredentials rootCert;
  final ReadWriteLock lock;

  public OneDBRpc(Party own, Set<Party> parties, ExecutorService threadPool,
      ChannelCredentials rootCert) {
    this.own = own;
    this.parties = parties;
    this.participantMap = new HashMap<>();
    this.clientMap = new HashMap<>();
    this.bufferMap = new HashMap<>();
    this.threadPool = threadPool;
    for (Party p : parties) {
      this.participantMap.put(p.getPartyId(), p);
      this.clientMap.put(p.getPartyId(), new PipeClient(own.getPartyName(), rootCert));
      this.bufferMap.put(p.getPartyId(), new ConcurrentBuffer<DataPacketHeader, DataPacket>());
    }
    this.gRpcService = new PipeService(bufferMap);
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
    this.rootCert = rootCert;
    this.lock = new ReentrantReadWriteLock();
  }

  public OneDBRpc(Party own, ExecutorService threadPool) {
    this(own, new HashSet<>(Arrays.asList(own)), threadPool, null);
  }

  public OneDBRpc(Party own, ExecutorService threadPool, ChannelCredentials rootCert) {
    this(own, new HashSet<>(Arrays.asList(own)), threadPool, rootCert);
  }

  public OneDBRpc(Party own, List<Party> parties, List<Channel> channels) {
    assert parties.size() == channels.size();
    this.own = own;
    this.parties = new HashSet<>();
    this.participantMap = new HashMap<>();
    this.clientMap = new HashMap<>();
    this.bufferMap = new HashMap<>();
    for (int i = 0; i < parties.size(); ++i) {
      Party p = parties.get(i);
      Channel ch = channels.get(i);
      this.parties.add(p);
      this.participantMap.put(p.getPartyId(), p);
      // if (!p.equals(own)) {
        this.clientMap.put(p.getPartyId(), new PipeClient(ch));
        this.bufferMap.put(p.getPartyId(), new ConcurrentBuffer<DataPacketHeader, DataPacket>());
      // }
    }
    this.gRpcService = new PipeService(bufferMap);
    this.threadPool = Executors.newFixedThreadPool(THREAD_NUM);
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
    this.rootCert = null;
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  public Party ownParty() {
    return own;
  }

  @Override
  public Set<Party> getPartySet() {
    return parties;
  }

  @Override
  public Party getParty(int partyId) {
    return participantMap.get(partyId);
  }

  @Override
  public void connect() {
    lock.readLock().lock();
    clientMap.values().stream().forEach(c -> c.connect());
    lock.readLock().unlock();
  }

  @Override
  public void send(DataPacket dataPacket) {
    int receiverId = dataPacket.getHeader().getReceiverId();
    int senderId = dataPacket.getHeader().getSenderId();
    assert senderId == own.getPartyId();
    lock.readLock().lock();
    PipeClient client = clientMap.get(receiverId);
    lock.readLock().unlock();
    if (client != null) {
      client.send(dataPacket.toProto());
      dataPacketNum++;
      payloadByteLength += dataPacket.getPayloadByteLength();
    } else {
      LOG.error("No connection to receiver[{}]", receiverId);
    }
  }

  @Override
  public DataPacket receive(DataPacketHeader header) {
    ConcurrentBuffer<DataPacketHeader, DataPacket> buffer = bufferMap.get(header.getSenderId());
    return buffer.blockingPop(header);
  }

  @Override
  public long getPayloadByteLength(boolean reset) {
    long length = payloadByteLength;
    if (reset) {
      payloadByteLength = 0;
    }
    return length;
  }

  @Override
  public long getSendDataPacketNum(boolean reset) {
    long number = dataPacketNum;
    if (reset) {
      dataPacketNum = 0;
    }
    return number;
  }

  @Override
  public void disconnect() {
    lock.readLock().lock();
    clientMap.values().forEach(c -> c.close());
    lock.readLock().unlock();
  }

  public BindableService getgRpcService() {
    return gRpcService;
  }

  public boolean addParty(Party party) {
    lock.writeLock().lock();
    if (parties.contains(party)) {
      LOG.warn("{} already exists", party);
      lock.writeLock().unlock();
      return false;
    }
    parties.add(party);
    participantMap.put(party.getPartyId(), party);
    clientMap.put(party.getPartyId(), new PipeClient(party.getPartyName(), rootCert));
    bufferMap.put(party.getPartyId(), new ConcurrentBuffer<DataPacketHeader, DataPacket>());
    lock.writeLock().unlock();
    return true;
  }

  public boolean removeParty(Party party) {
    lock.writeLock().lock();
    if (!parties.contains(party)) {
      LOG.warn("{} not exists", party);
      lock.writeLock().unlock();
      return false;
    }
    parties.remove(party);
    participantMap.remove(party.getPartyId());
    clientMap.remove(party.getPartyId());
    bufferMap.remove(party.getPartyId());
    lock.writeLock().unlock();
    return true;
  }
}
