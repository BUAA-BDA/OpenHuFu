package com.hufudb.onedb.core.table;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.TableMeta.LocalTableMeta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBTableInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchema.class);
  private final ReadWriteLock lock;
  private final String name;
  private final Header header;
  private final Map<String, Integer> columnMap;
  private List<Pair<OwnerClient, String>> tableList;

  public OneDBTableInfo(String name, Header header) {
    this.name = name;
    this.header = header;
    this.tableList = new ArrayList<>();
    this.columnMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    for (int i = 0; i < header.size(); ++i) {
      columnMap.put(header.getName(i), i);
    }
  }

  public OneDBTableInfo(String globalName, Header header, OwnerClient client, String localName) {
    this(globalName, header);
    this.tableList.add(Pair.of(client, localName));
  }

  public void addLocalTable(OwnerClient client, String localName) {
    Header header = client.getTableHeader(localName);
    if (header.equals(this.header)) {
      lock.writeLock().lock();
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn(
          "Table {} header {} mismatch with global table {} header {}",
          localName,
          header,
          name,
          this.header);
    }
  }

  public void changeLocalTable(OwnerClient client, String localName) {
    Header header = client.getTableHeader(localName);
    if (header.equals(this.header)) {
      lock.writeLock().lock();
      for (int i = 0; i < tableList.size(); ++i) {
        Pair<OwnerClient, String> pair = tableList.get(i);
        if (pair.left.equals(client)) {
          tableList.remove(i);
          break;
        }
      }
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn(
          "Table {} header {} mismatch with global table {} header {}",
          localName,
          header,
          name,
          this.header);
    }
  }

  public Header getHeader() {
    return header;
  }

  public String getName() {
    return name;
  }

  public List<Pair<OwnerClient, String>> getTableList() {
    try {
      lock.readLock().lock();
      return ImmutableList.copyOf(tableList);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int ownerSize() {
    try {
      lock.readLock().lock();
      return tableList.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Set<OwnerClient> getOwners() {
    lock.readLock().lock();
    Set<OwnerClient> owners = tableList.stream().map(p -> p.getKey()).collect(Collectors.toSet());
    lock.readLock().unlock();
    return owners;
  }

  public void removeOwner(OwnerClient client) {
    List<Pair<OwnerClient, String>> newList = new ArrayList<>();
    lock.readLock().lock();
    for (Pair<OwnerClient, String> pair : tableList) {
      if (!pair.left.equals(client)) {
        newList.add(pair);
      }
    }
    lock.readLock().unlock();
    lock.writeLock().lock();
    tableList = newList;
    lock.writeLock().unlock();
  }

  public void dropLocalTable(OwnerClient client, String localName) {
    lock.writeLock().lock();
    for (int i = 0; i < tableList.size(); ++i) {
      Pair<OwnerClient, String> pair = tableList.get(i);
      if (pair.left.equals(client) && pair.right.equals(localName)) {
        tableList.remove(i);
        break;
      }
    }
    lock.writeLock().unlock();
  }

  public void dropLocalTable(OwnerClient client) {
    lock.writeLock().lock();
    for (int i = 0; i < tableList.size(); ++i) {
      Pair<OwnerClient, String> pair = tableList.get(i);
      if (pair.left.equals(client)) {
        tableList.remove(i);
        break;
      }
    }
    lock.writeLock().unlock();
  }

  public List<String> getEndpoints() {
    List<String> endpoints = new ArrayList<>();
    lock.readLock().lock();
    endpoints =
        tableList.stream().map(pair -> pair.getKey().getEndpoint()).collect(Collectors.toList());
    lock.readLock().unlock();
    return endpoints;
  }

  public List<LocalTableMeta> getMappings() {
    return getTableList().stream()
        .map(p -> new LocalTableMeta(p.left.getEndpoint(), p.right))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    List<String> mappings =
        getMappings().stream().map(p -> p.toString()).collect(Collectors.toList());
    return String.format("[%s](%s){%s}", name, header, StringUtils.join(mappings, ","));
  }
}
