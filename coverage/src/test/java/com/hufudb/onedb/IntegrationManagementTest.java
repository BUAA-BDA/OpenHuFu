package com.hufudb.onedb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.user.OneDB;

@RunWith(JUnit4.class)
public class IntegrationManagementTest {
  static ExecutorService rootContext;
  static int globalTableNum = 0;
  static List<String> dockerOwnerConfigs =
      ImmutableList.of("docker/owner1.json", "docker/owner2.json", "docker/owner3.json");
  static List<String> localOwnerConfigs =
      ImmutableList.of("local/owner1.json", "local/owner2.json", "local/owner3.json");
  static boolean useDocker = true;

  static {
    if (System.getenv("ONEDB_TEST_LOCAL") != null) {
      useDocker = false;
    }
  }

  static List<OwnerServer> initAllOwner() throws Exception {
    List<String> configs = useDocker ? dockerOwnerConfigs : localOwnerConfigs;
    rootContext = Executors.newFixedThreadPool(configs.size());
    List<OwnerServer> owners = new ArrayList<>();
    for (String config : configs) {
      URL ownerConfig = IntegrationManagementTest.class.getClassLoader().getResource(config);
      OwnerServer owner = OwnerServer.create(ownerConfig.getPath());
      Future<?> future = rootContext.submit(new Runnable() {
        @Override
        public void run() {
          try {
            owner.start();
          } catch (Exception e) {
            e.printStackTrace();
            assertTrue("Owner failed", false);
          }
        }
      });
      future.get();
      owners.add(owner);
    }
    return owners;
  }

  @Test
  public void ownerManagementTest() throws Exception {
    List<OwnerServer> owners = initAllOwner();
    OwnerServer o1 = owners.get(0);
    OwnerService os1 = o1.getService();
    assertTrue(os1.getAllLocalTable().size() > 0);
    assertTrue(os1.getAllPublishedTable().size() > 0);
    TableSchema s1 = os1.getLocalTableSchema("student");
    assertEquals("student", s1.getName());
    assertEquals(4, s1.getSchema().size());
    os1.clearPublishedTable();
    assertTrue(os1.getAllPublishedTable().isEmpty());
    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("student");
    t1.setPublishedName("student1");
    t1.setPublishedColumns(ImmutableList.of());
    t1.setActualColumns(ImmutableList.of());
    assertTrue(os1.addPublishedTable(t1));
    assertFalse(os1.addPublishedTable(t1));
    assertTrue(os1.getAllPublishedTable().size() == 1);
    os1.dropPublishedTable("student1");
    assertTrue(os1.getAllPublishedTable().isEmpty());
    assertFalse(os1.changeCatalog("unsupport"));
    for (OwnerServer owner : owners) {
      owner.stop();
    }
    Thread.sleep(1000);
  }

  static OneDB initUser(List<OwnerServer> owners) throws Exception {
    OneDB user = new OneDB();
    globalTableNum = 0;
    for (OwnerServer owner : owners) {
      user.addOwner(owner.getEndpoint());
    }
    URL userConfig = IntegrationManagementTest.class.getClassLoader().getResource("user.json");
    Reader reader = Files.newBufferedReader(Paths.get(userConfig.getPath()));
    List<GlobalTableConfig> userTableConfig =
        new Gson().fromJson(reader, new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
    for (GlobalTableConfig config : userTableConfig) {
      assertTrue(user.createOneDBTable(config));
      globalTableNum++;
    }
    return user;
  }

  @Test
  public void userManagementTest() throws Exception {
    List<OwnerServer> owners = initAllOwner();
    OneDB user = initUser(owners);
    Set<String> endpoints = user.getEndpoints();
    for (OwnerServer owner : owners) {
      assertTrue(endpoints.contains(owner.getEndpoint()));
    }
    OwnerServer owner1 = owners.get(0);
    assertTrue(user.getOwnerTableSchema(owner1.getEndpoint()).size() > 0);
    assertEquals(globalTableNum, user.getAllOneDBTableSchema().size());
    assertTrue("Error when add a existing owner", user.addOwner(owner1.getEndpoint()));
    user.removeOwner(owner1.getEndpoint());
    assertEquals(2, user.getOneDBTableSchema("student_pub_share").getOwners().size());
    assertEquals(2, user.getOneDBTableSchema("student_pub_share").getEndpoints().size());
    assertEquals(2, user.getOneDBTableSchema("student_pub_share").getMappings().size());
    assertNull(user.getOneDBTableSchema("student_pub_s1"));
    assertEquals(ImmutableList.of(), user.getOwnerTableSchema("region"));
    assertNull(user.getOneDBTableSchema("region"));
    user.addOwner("localhost:6789");
    assertFalse(user.addLocalTable("no_exist", "localhost:6789", "student1"));
    assertFalse(user.addLocalTable("student_pub_share", "not_exist", "student1"));
    assertFalse(user.addLocalTable("student_pub_share", "localhost:6789", "not_exist"));
    assertTrue(user.addLocalTable("student_pub_share", "localhost:6789", "student1"));
    assertEquals(3, user.getOneDBTableSchema("student_pub_share").getEndpoints().size());
    user.dropLocalTable("student_pub_share", "localhost:6789", "student1");
    assertEquals(2, user.getOneDBTableSchema("student_pub_share").localTableNumber());
    user.dropOneDBTable("not_exist");
    user.dropOneDBTable("student_pub_s2");
    assertNull(user.getOneDBTableSchema("student_pub_s2"));
    GlobalTableConfig wrongConfig =new GlobalTableConfig("student_pub_s2", ImmutableList.of(new LocalTableConfig("localhost:6790", "not_exist")));
    assertFalse(user.createOneDBTable(wrongConfig));
    GlobalTableConfig rightConfig =new GlobalTableConfig("student_pub_s2", ImmutableList.of(new LocalTableConfig("localhost:6790", "student1")));
    assertTrue(user.createOneDBTable(rightConfig));
    assertNotNull(user.getOneDBTableSchema("student_pub_s2"));
    user.close();
    for (OwnerServer owner : owners) {
      owner.stop();
    }
    Thread.sleep(1000);
  }

  @Test
  public void jdbcTest() throws Exception {
    List<OwnerServer> owners = initAllOwner();
    Class.forName("com.hufudb.onedb.user.jdbc.OneDBDriver");
    List<String> dbargs = new ArrayList<>();
    String m = IntegrationManagementTest.class.getClassLoader().getResource("model.json").getPath();
    dbargs.add("-u");
    dbargs.add("jdbc:onedb:model=" + m + ";lex=JAVA;caseSensitive=false;");
    dbargs.add("-n");
    dbargs.add("admin");
    dbargs.add("-p");
    dbargs.add("admin");
    Connection connection = DriverManager.getConnection("jdbc:onedb:model=" + m + ";lex=JAVA;caseSensitive=false;", "admin", "admin");
    Statement statement = connection.createStatement();
    ResultSet result = statement.executeQuery("select * from student");
    int count = 0;
    while (result.next()) {
      count++;
    }
    assertTrue(count > 0);
    result.close();
    statement.close();
    connection.close();
    for (OwnerServer owner : owners) {
      owner.stop();
    }
    Thread.sleep(1000);
  }
}
