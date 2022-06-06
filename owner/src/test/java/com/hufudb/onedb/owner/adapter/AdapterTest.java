package com.hufudb.onedb.owner.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.LeafPlan;

@RunWith(JUnit4.class)
public class AdapterTest {

  static List<PojoPublishedTableSchema> publishedSchemas;

  static {
    PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
    t1.setActualName("student");
    t1.setPublishedName("student1");
    t1.setPublishedColumns(ImmutableList.of());
    t1.setActualColumns(ImmutableList.of());
    PojoPublishedTableSchema t2 = new PojoPublishedTableSchema();
    t2.setActualName("student");
    t2.setPublishedName("student2");
    t2.setPublishedColumns(ImmutableList.of(
        new PojoColumnDesc("name", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("score", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC),
        new PojoColumnDesc("age", ColumnTypeWrapper.INT, ModifierWrapper.HIDDEN),
        new PojoColumnDesc("dept_name", ColumnTypeWrapper.STRING, ModifierWrapper.HIDDEN)));
    t2.setActualColumns(ImmutableList.of(0, 2, 1, 3));
    publishedSchemas = ImmutableList.of(t1, t2);
  }

  Adapter loadAdapter() {
    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.datasource = "postgresql";
    adapterConfig.url = "jdbc:postgresql://postgres1:5432/postgres";
    adapterConfig.catalog = "postgres";
    adapterConfig.user = "postgres";
    adapterConfig.passwd = "onedb";
    String onedbRoot = System.getenv("ONEDB_ROOT");
    assertNotNull("ONEDB_ROOT env variable is not set", onedbRoot);
    String adapterDir = Paths.get(onedbRoot, "adapter").toString();
    return AdapterFactory.loadAdapter(adapterConfig, adapterDir);
  }

  @Test
  public void testAdapter() {
    Adapter adapter = loadAdapter();
    // test schema manager
    SchemaManager manager = adapter.getSchemaManager();
    for (PojoPublishedTableSchema schema : publishedSchemas) {
      assertTrue(manager.addPublishedTable(schema));
    }
    // test query select * from student1;
    LeafPlan plan = new LeafPlan();
    plan.setTableName("student1");
    plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("student1")));
    DataSet result = adapter.query(plan);
    DataSetIterator it = result.getIterator();
    int count = 0;
    while (it.next()) {
      count++;
    }
    assertEquals(3, count);
    // todo: test more query plan
  }
}
