package com.hufudb.onedb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.user.OneDB;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IntegrationQueryTest {

  static List<OwnerServer> owners;
  static OneDB user;
  static ExecutorService rootContext;
  static int globalTableNum = 0;
  static List<String> dockerOwnerConfigs =
      ImmutableList.of("docker/owner1.json", "docker/owner2.json", "docker/owner3.json");
  static List<String> localOwnerConfigs =
      ImmutableList.of("local/owner1.json", "local/owner2.json", "local/owner3.json");

  static void initUser() throws Exception {
    user = new OneDB();
    globalTableNum = 0;
    for (OwnerServer owner : owners) {
      user.addOwner(owner.getEndpoint());
    }
    URL userConfig = IntegrationQueryTest.class.getClassLoader().getResource("user.json");
    Reader reader = Files.newBufferedReader(Paths.get(userConfig.getPath()));
    List<GlobalTableConfig> userTableConfig =
        new Gson().fromJson(reader, new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
    for (GlobalTableConfig config : userTableConfig) {
      assertTrue(user.createOneDBTable(config));
      globalTableNum++;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    boolean useDocker = true;
    if (System.getenv("ONEDB_TEST_LOCAL") != null) {
      useDocker = false;
    }
    List<String> configs = useDocker ? dockerOwnerConfigs : localOwnerConfigs;
    rootContext = Executors.newFixedThreadPool(configs.size());
    owners = new ArrayList<>();
    for (String config : configs) {
      URL ownerConfig = IntegrationQueryTest.class.getClassLoader().getResource(config);
      OwnerServer owner = OwnerServer.create(ownerConfig.getPath());
      Future<?> future = rootContext.submit(new Runnable() {
        @Override
        public void run() {
          try {
            owner.start();
          } catch (Exception e) {
            assertTrue("Owner failed", false);
          }
        }
      });
      future.get();
      owners.add(owner);
    }
    initUser();
  }

  static List<ArrayRow> toRows(ResultSet rs) throws SQLException {
    List<ArrayRow> result = new ArrayList<>();
    while (rs.next()) {
      ArrayRow.Builder builder = ArrayRow.newBuilder(rs.getMetaData().getColumnCount());
      for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
        builder.set(i, rs.getObject(i + 1));
      }
      result.add(builder.build());
    }
    return result;
  }

  static List<ArrayRow> toRows(List<List<Object>> objects) {
    int rowNum = objects.size();
    List<ArrayRow> rows = new ArrayList<>();
    if (rowNum == 0) {
      return rows;
    }
    int colNum = objects.get(0).size();
    for (int i = 0; i < rowNum; ++i) {
      ArrayRow.Builder builder = ArrayRow.newBuilder(colNum);
      for (int j = 0; j < colNum; ++j) {
        builder.set(j, objects.get(i).get(j));
      }
      rows.add(builder.build());
    }
    return rows;
  }

  static List<ArrayRow> toRows(DataSet dataSet) {
    DataSetIterator it = dataSet.getIterator();
    List<ArrayRow> rows = new ArrayList<>();
    while (it.next()) {
      int colNum = it.size();
      ArrayRow.Builder builder = ArrayRow.newBuilder(colNum);
      for (int j = 0; j < colNum; ++j) {
        builder.set(j, it.get(j));
      }
      rows.add(builder.build());
    }
    return rows;
  }

  private static int compare(ArrayRow row1, ArrayRow row2) {
    int compareResult = 0;
    assert row1.size() == row2.size();
    for (int i = 0; i < row1.size(); i++) {
      compareResult = ((Comparable) row1.get(i)).compareTo((Comparable) row2.get(i));
      if ((row1.get(i) instanceof Float) || (row1.get(i) instanceof Double)) {
        double x = (Double) row1.get(i);
        double y = (Double) row2.get(i);
        if (Math.abs(x - y) < 1e-5) {
          compareResult = 0;
        }
      }
      if (compareResult != 0) {
        return compareResult;
      }
    }
    return compareResult;
  }

  private static boolean equals(ArrayRow row1, ArrayRow row2) {
    if (row1.size() != row2.size()) {
      return false;
    }
    for (int i = 0; i < row1.size(); ++i) {
      Object o1 = row1.get(i);
      Object o2 = row2.get(i);
      if (o1 == null || o2 == null) {
        if (!(o1 == null && o2 == null)) {
          return false;
        }
      } else if (o1 instanceof Long || o1 instanceof Integer) {
        if (((Number) o1).longValue() != ((Number) o2).longValue()) {
          return false;
        }
      } else if (o1 instanceof Float || o1 instanceof Double) {
        if (((Number) o1).doubleValue() != ((Number) o2).doubleValue()) {
          return false;
        }
      } else if (o1 instanceof Date || o1 instanceof Time || o1 instanceof Timestamp) {
        if (!o1.toString().equals(o2.toString())) {
          return false;
        }
      } else if (!o1.equals(o2)){
        return false;
      }
    }
    return true;
  }

  static void compareRows(List<ArrayRow> expect, List<ArrayRow> actual) {
    assertEquals(expect.size(), actual.size());
    expect.sort(IntegrationQueryTest::compare);
    actual.sort(IntegrationQueryTest::compare);
    for (int i = 0; i < expect.size(); ++i) {
      assertTrue(equals(expect.get(i), actual.get(i)));
    }
  }

  static void compareRowsWithOrder(List<ArrayRow> expect, List<ArrayRow> actual) {
    assertEquals(expect.size(), actual.size());
    for (int i = 0; i < expect.size(); ++i) {
      assertTrue(equals(expect.get(i), actual.get(i)));
    }
  }

  @Test
  public void basicTest() throws Exception {
    ResultSet result = user.executeQuery("select * from student_pub_share");
    List<ArrayRow> expect = toRows(ImmutableList.of(ImmutableList.of("tom", 21, 90, "computer"),
        ImmutableList.of("anna", 20, 89, "software"), ImmutableList.of("Snow", 20, 99, "software"),
        ImmutableList.of("peter", 20, 71, "computer"), ImmutableList.of("mary", 22, 82, "math"),
        ImmutableList.of("Brown", 21, 88, "software"),
        ImmutableList.of("john", 22, 100, "computer"), ImmutableList.of("jack", 23, 60, "physics"),
        ImmutableList.of("Brand", 20, 80, "electronics")));
    compareRows(expect, toRows(result));
    result.close();

    LeafPlan plan = new LeafPlan();
    plan.setTableName("student_pub_share");
    plan.setSelectExps(ExpressionFactory
        .createInputRef(user.getOneDBTableSchema("student_pub_share").getSchema()));
    DataSet dataset = user.executeQuery(plan);
    compareRows(expect, toRows(dataset));

    result = user.executeQuery("select * from student_pub_s1");
    expect = toRows(ImmutableList.of(ImmutableList.of("tom", 21, 90, "computer"),
        ImmutableList.of("anna", 20, 89, "software"),
        ImmutableList.of("Snow", 20, 99, "software")));
    compareRows(expect, toRows(result));
    result.close();
    result = user.executeQuery("select * from not_exist");
    assertNull(result);
  }


  @Test
  public void simpleAggregateTest() throws Exception {
    ResultSet result = user.executeQuery("select avg(score) from student_pub_share");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of(84)
    ));
    List<ArrayRow> act = toRows(result);
    compareRows(expect, act);
    result.close();

    result = user.executeQuery("select COUNT(*) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(9)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(9)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(distinct score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(9)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(age) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(9)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(distinct age) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(4)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(distinct age, score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(9)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select MAX(score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(100)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select MAX(distinct score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(100)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select MIN(score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(60)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select MIN(distinct score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(60)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select SUM(score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(759)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select SUM(distinct score) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(759)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select SUM(age) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(189)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select SUM(distinct age) from student_pub_share");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(86)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(age) from student_pub_s1");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(3)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(distinct age) from student_pub_s1");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(2)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select avg(score) from student_pub_s1");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(92)
    ));
    compareRows(expect, toRows(result));
    result.close();
  }

  @Test
  public void likeTest() throws SQLException {
    ResultSet result = user.executeQuery("select name from student_pub_share where dept_name like 'software'");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("anna"),
      ImmutableList.of("Snow"),
      ImmutableList.of("Brown")
    ));
    List<ArrayRow> act = toRows(result);
    compareRows(expect, act);
    result.close();

    result  = user.executeQuery("select name from student_pub_share where dept_name like 'e%r'");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("tom"),
      ImmutableList.of("peter"),
      ImmutableList.of("john"),
      ImmutableList.of("Brand")
    ));
    act = toRows(result);
    compareRows(expect, act);
    result.close();

    result  = user.executeQuery("select name from student_pub_share where dept_name like 'r__ics'");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("Brand")
    ));
    act = toRows(result);
    compareRows(expect, act);
    result.close();
  }

  @Test
  public void groupByTest() throws SQLException {
    ResultSet result = user.executeQuery("select AVG(score) from student_pub_share group by dept_name");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of(82),
      ImmutableList.of(60),
      ImmutableList.of(92),
      ImmutableList.of(80),
      ImmutableList.of(87)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select AVG(score), dept_name from student_pub_share group by dept_name");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(82, "math"),
      ImmutableList.of(60, "physics"),
      ImmutableList.of(92, "software"),
      ImmutableList.of(80, "electronics"),
      ImmutableList.of(87, "computer")
    ));
    compareRows(expect, toRows(result));
    result.close();

    // todo: add more test
  }

  @Test
  public void whereTest() throws SQLException {
    ResultSet result = user.executeQuery("select name, score from student_pub_share where score >= 95");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("Snow", 99),
      ImmutableList.of("john", 100)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select name, score from student_pub_share where score < 70");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("jack", 60)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select name, score from student_pub_share where score = 100");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("john", 100)
    ));
    compareRows(expect, toRows(result));
    result.close();
    // todo: add more test
  }

  @Test
  public void calcTest() throws SQLException {
    ResultSet result = user.executeQuery("select name, score + 10 from student_pub_s1");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("tom", 100),
      ImmutableList.of("anna", 99),
      ImmutableList.of("Snow", 109)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select name, -age from student_pub_s1");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("tom", -21),
      ImmutableList.of("anna", -20),
      ImmutableList.of("Snow", -20)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select name, case when score < 70 then score + 10 when score <= 80 then score + 5 else score end from student_pub_s3");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("john", 100),
      ImmutableList.of("jack", 70),
      ImmutableList.of("Brand", 85)
    ));
    compareRows(expect, toRows(result));
    result.close();
    // todo: add more test
  }

  @Test
  public void joinTest() throws SQLException {
    ResultSet result = user.executeQuery("select student_pub_s1.name, student_pub_s2.name from student_pub_s1, student_pub_s2 where student_pub_s1.age > student_pub_s2.age");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("tom", "peter")
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select student_pub_s1.name, student_pub_s2.name from student_pub_s1, student_pub_s2 where student_pub_s1.dept_name = student_pub_s2.dept_name");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("tom", "peter"),
      ImmutableList.of("anna", "Brown"),
      ImmutableList.of("Snow", "Brown")
    ));
    compareRows(expect, toRows(result));
    result.close();
    // todo: add more test
  }

  @Test
  public void orderByTest() throws SQLException {
    ResultSet result = user.executeQuery("select name, score from student_pub_share order by score");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("jack", 60),
      ImmutableList.of("peter", 71),
      ImmutableList.of("Brand", 80),
      ImmutableList.of("mary", 82),
      ImmutableList.of("Brown", 88),
      ImmutableList.of("anna", 89),
      ImmutableList.of("tom", 90),
      ImmutableList.of("Snow", 99),
      ImmutableList.of("john", 100)
    ));
    compareRowsWithOrder(expect, toRows(result));
    result.close();

    result = user.executeQuery("select name, score from student_pub_share order by score desc");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("john", 100),
      ImmutableList.of("Snow", 99),
      ImmutableList.of("tom", 90),
      ImmutableList.of("anna", 89),
      ImmutableList.of("Brown", 88),
      ImmutableList.of("mary", 82),
      ImmutableList.of("Brand", 80),
      ImmutableList.of("peter", 71),
      ImmutableList.of("jack", 60)
    ));
    compareRowsWithOrder(expect, toRows(result));
    result.close();

    // todo: add more test
  }

  @Test
  public void dateTimeTest() throws SQLException {
    ResultSet result = user.executeQuery("select license, time_stamp from time_table where time_stamp < timestamp '2020-04-21 15:30:00'");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("10004", Timestamp.valueOf("2019-11-28 11:20:43")),
      ImmutableList.of("10005", Timestamp.valueOf("2019-10-15 16:51:32")),
      ImmutableList.of("10000", Timestamp.valueOf("2018-09-01 09:05:10")),
      ImmutableList.of("10001", Timestamp.valueOf("2018-06-01 10:14:45")),
      ImmutableList.of("10002", Timestamp.valueOf("2019-01-30 21:31:20"))
    ));
    compareRows(expect, toRows(result));

    result = user.executeQuery("select license, cur_time from time_table where cur_time < time '15:00:00.0'");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("10000", Time.valueOf("09:05:10")),
      ImmutableList.of("10001", Time.valueOf("10:14:45")),
      ImmutableList.of("10004", Time.valueOf("11:20:43"))
    ));
    compareRows(expect, toRows(result));

    result = user.executeQuery("select license, cur_date from time_table where cur_date < date '2019-01-01'");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("10000", Date.valueOf("2018-09-01")),
      ImmutableList.of("10001", Date.valueOf("2018-06-01"))
    ));
    compareRows(expect, toRows(result));
  }

  @Test
  public void protectedJoinTest() throws SQLException {
    ResultSet result = user.executeQuery("select student_pro_s1.name, student_pro_s2.name from student_pro_s1, student_pro_s2 where student_pro_s1.dept_name = student_pro_s2.dept_name");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of("tom", "peter"),
      ImmutableList.of("anna", "Brown"),
      ImmutableList.of("Snow", "Brown")
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select student_pro_s2.name, student_pro_s1.name from student_pro_s1, student_pro_s2 where student_pro_s1.dept_name = student_pro_s2.dept_name");
    expect = toRows(ImmutableList.of(
      ImmutableList.of("peter", "tom"),
      ImmutableList.of("Brown", "anna"),
      ImmutableList.of("Brown", "Snow")
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select student_pro_s2.name, student_pro_s1.name from student_pro_s1, student_pro_s2 where student_pro_s1.score = student_pro_s2.score");
    expect = toRows(ImmutableList.of(
    ));
    compareRows(expect, toRows(result));
    result.close();

    // todo: add more test
  }

  @Test
  public void protectedSumTest() throws SQLException {
    ResultSet result = user.executeQuery("select SUM(score) from student_pro_share2");
    List<ArrayRow> expect = toRows(ImmutableList.of(
      ImmutableList.of(519)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(*) from student_pro_share2");
    expect = toRows(ImmutableList.of(
      ImmutableList.of(6)
    ));
    compareRows(expect, toRows(result));
    result.close();

    result = user.executeQuery("select COUNT(*) from student_pro_share3");
    expect = toRows(ImmutableList.of(ImmutableList.of(9)));
    compareRows(expect, toRows(result));

    result = user.executeQuery("select SUM(score) from student_pro_share3");
    expect = toRows(ImmutableList.of(ImmutableList.of(759)));
    compareRows(expect, toRows(result));
    // todo: add more test
  }

  @AfterClass
  public static void shutdown() throws Exception {
    user.close();
    for (OwnerServer owner : owners) {
      owner.stop();
    }
  }
}
