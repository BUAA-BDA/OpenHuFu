package com.hufudb.onedb.core.table;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.data.schema.Schema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;

public class OneDBTableSchema {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBTableSchema.class);
  private final ReadWriteLock lock;
  private final String name;
  private final Schema schema;
  private final Map<String, Integer> columnMap;
  private List<Pair<OwnerClient, String>> tableList;

  public OneDBTableSchema(String name, Schema schema) {
    this.name = name;
    this.schema = schema;
    this.tableList = new ArrayList<>();
    this.columnMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    for (int i = 0; i < schema.size(); ++i) {
      columnMap.put(schema.getName(i), i);
    }
  }

  public OneDBTableSchema(String globalName, Schema schema, OwnerClient client, String localName) {
    this(globalName, schema);
    this.tableList.add(Pair.of(client, localName));
  }

  public void addLocalTable(OwnerClient client, String localName) {
    Schema schema = client.getTableSchema(localName);
    if (schema.equals(this.schema)) {
      lock.writeLock().lock();
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn("Table {} schema {} mismatch with global table {} schema {}", localName, schema,
          name, this.schema);
    }
  }

  public void changeLocalTable(OwnerClient client, String localName) {
    Schema schema = client.getTableSchema(localName);
    if (schema.equals(this.schema)) {
      lock.writeLock().lock();
      for (int i = 0; i < tableList.size(); ++i) {
        Pair<OwnerClient, String> pair = tableList.get(i);
        if (pair.getLeft().equals(client)) {
          tableList.remove(i);
          break;
        }
      }
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn("Table {} schema {} mismatch with global table {} schema {}", localName, schema,
          name, this.schema);
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public String getName() {
    return name;
  }

  public Integer getColumnId(String columnName) {
    if (!columnMap.containsKey(columnName)) {
      LOG.warn("Column {} not exists in {}", columnName, name);
      throw new RuntimeException("Column  not exists");
    }
    return columnMap.get(columnName);
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
      if (!pair.getLeft().equals(client)) {
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
      if (pair.getLeft().equals(client) && pair.getRight().equals(localName)) {
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
      if (pair.getLeft().equals(client)) {
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

  public List<LocalTableConfig> getMappings() {
    return getTableList().stream()
        .map(p -> new LocalTableConfig(p.getLeft().getEndpoint(), p.getRight()))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    List<String> mappings =
        getMappings().stream().map(p -> p.toString()).collect(Collectors.toList());
    return String.format("[%s](%s){%s}", name, schema, StringUtils.join(mappings, ","));
  }
}
