package tk.onedb.core.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.util.Pair;

import tk.onedb.core.client.DBClient;
import tk.onedb.core.data.Header;

public class OneDBTableInfo {
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
    lock.writeLock().lock();
    tableList.add(Pair.of(client, localName));
    lock.writeLock().unlock();
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

  public void removeDB(DBClient client) {
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

  public void removeLocalTable(DBClient client, String localName) {
    lock.readLock().lock();
    for (int i = 0; i < tableList.size(); ++i) {
      Pair<DBClient, String> pair = tableList.get(i);
      if (pair.left.equals(client) && pair.right.equals(localName)) {
        tableList.remove(i);
        break;
      }
    }
    lock.readLock().unlock();
  }
}
