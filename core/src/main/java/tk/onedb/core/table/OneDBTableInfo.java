package tk.onedb.core.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.client.DBClient;
import tk.onedb.core.data.Header;
import tk.onedb.core.sql.schema.OneDBSchema;
public class OneDBTableInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchema.class);

  private String name;
  private Header header;
  private Map<String, Integer> columnMap;
  private List<Pair<DBClient, String>> tableList;
  private final ReadWriteLock lock;

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

  public OneDBTableInfo(String globalName, Header header, DBClient client, String localName) {
    this(globalName, header);
    this.tableList.add(Pair.of(client, localName));
  }

  public void addLocalTable(DBClient client, String localName) {
    Header header = client.getTableHeader(localName);
    if (header.equals(this.header)) {
      lock.writeLock().lock();
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn("Table {} header {} mismatch with global table {} header {}", localName, header, name, this.header);
    }
  }

  public void changeLocalTable(DBClient client, String localName) {
    Header header = client.getTableHeader(localName);
    if (header.equals(this.header)) {
      lock.readLock().lock();
      for (int i = 0; i < tableList.size(); ++i) {
        Pair<DBClient, String> pair = tableList.get(i);
        if (pair.left.equals(client)) {
          lock.writeLock().lock();
          tableList.remove(i);
          lock.writeLock().unlock();
          break;
        }
      }
      lock.readLock().unlock();
      lock.writeLock().lock();
      tableList.add(Pair.of(client, localName));
      lock.writeLock().unlock();
    } else {
      LOG.warn("Table {} header {} mismatch with global table {} header {}", localName, header, name, this.header);
    }
  }

  public Header getHeader() {
    return header;
  }

  public String getName() {
    return name;
  }

  public List<Pair<DBClient, String>> getTableList() {
    try {
      lock.readLock().lock();
      return ImmutableList.copyOf(tableList);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void dropDB(DBClient client) {
    List<Pair<DBClient, String>> newList = new ArrayList<>();
    lock.readLock().lock();
    for (Pair<DBClient, String> pair : tableList) {
      if (!pair.left.equals(client)) {
        newList.add(pair);
      }
    }
    lock.readLock().unlock();
    lock.writeLock().lock();
    tableList = newList;
    lock.writeLock().unlock();
  }

  public void dropLocalTable(DBClient client, String localName) {
    lock.readLock().lock();
    for (int i = 0; i < tableList.size(); ++i) {
      Pair<DBClient, String> pair = tableList.get(i);
      if (pair.left.equals(client) && pair.right.equals(localName)) {
        lock.writeLock().lock();
        tableList.remove(i);
        lock.writeLock().unlock();
        break;
      }
    }
    lock.readLock().unlock();
  }

  public void dropLocalTable(DBClient client) {
    lock.readLock().lock();
    for (int i = 0; i < tableList.size(); ++i) {
      Pair<DBClient, String> pair = tableList.get(i);
      if (pair.left.equals(client)) {
        lock.writeLock().lock();
        tableList.remove(i);
        lock.writeLock().unlock();
        break;
      }
    }
    lock.readLock().unlock();
  }

  public List<String> getEndpoints() {
    List<String> endpoints = new ArrayList<>();
    lock.readLock().lock();
    endpoints = tableList.stream().map(pair -> pair.getKey().getEndpoint()).collect(Collectors.toList());
    lock.readLock().unlock();
    return endpoints;
  }
}
