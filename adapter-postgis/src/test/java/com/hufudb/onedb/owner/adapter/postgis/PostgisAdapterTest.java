package com.hufudb.onedb.owner.adapter.postgis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.*;
import com.hufudb.onedb.data.storage.utils.*;
import com.hufudb.onedb.expression.*;
import com.hufudb.onedb.owner.adapter.*;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class PostgisAdapterTest {
    static List<PojoPublishedTableSchema> publishedSchemas;
    static PostgisAdapter adapter;
    static SchemaManager manager;

    @Ignore
    @BeforeClass
    public static void setUp() {
        boolean useDocker = true;
        if (System.getenv("ONEDB_TEST_LOCAL") != null) {
            useDocker = false;
        }

        AdapterConfig adapterConfig = new AdapterConfig();
        adapterConfig.datasource = "postgresql";
        if (useDocker) {
        adapterConfig.url = "jdbc:postgresql://postgres1:5432/postgres";
        } else {
        adapterConfig.url = "jdbc:postgresql://localhost:13101/postgres";
        }
        adapterConfig.catalog = "postgres";
        adapterConfig.user = "postgres";
        adapterConfig.passwd = "onedb";
        String onedbRoot = System.getenv("ONEDB_ROOT");
        assertNotNull("ONEDB_ROOT env variable is not set", onedbRoot);
        String adapterDir = Paths.get(onedbRoot, "adapter").toString();
        adapter = (PostgisAdapter) AdapterFactory.loadAdapter(adapterConfig, adapterDir);
        manager = adapter.getSchemaManager();

        PojoPublishedTableSchema t1 = new PojoPublishedTableSchema();
        t1.setActualName("student");
        t1.setPublishedName("student1");
        t1.setPublishedColumns(ImmutableList.of());
        t1.setActualColumns(ImmutableList.of());
        PojoPublishedTableSchema t2 = new PojoPublishedTableSchema();
        t2.setActualName("student");
        t2.setPublishedName("student2");
        t2.setPublishedColumns(ImmutableList.of(
            new PojoColumnDesc("DeptName", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
            new PojoColumnDesc("Score", ColumnTypeWrapper.INT, ModifierWrapper.PUBLIC),
            new PojoColumnDesc("Name", ColumnTypeWrapper.STRING, ModifierWrapper.PUBLIC),
            new PojoColumnDesc("Age", ColumnTypeWrapper.INT, ModifierWrapper.HIDDEN)));
        t2.setActualColumns(ImmutableList.of(3, 2, 0, 1));
        publishedSchemas = ImmutableList.of(t1, t2);

        PojoPublishedTableSchema t3 = new PojoPublishedTableSchema();
        t3.setActualName("taxi");
        t3.setPublishedName("taxi");
        t3.setPublishedColumns(ImmutableList.of());
        t3.setActualColumns(ImmutableList.of());
        assertTrue(manager.addPublishedTable(t3));
    }

    @Ignore
    @Test
    public void testGenerateSQL() {
        LeafPlan plan = new LeafPlan();
        /**
         * Select name from street where ST_Distance(geom, ST_GeomFromText('POINT(114, 514)', 4326)) >= 90;
         */
        // plan.setTableName("street");
        // plan.setSelectExps(ExpressionFactory.createInputRef(manager.getPublishedSchema("name")));
        // plan.setWhereExps(
        //     ImmutableList.of(
        //         ExpressionFactory.createBinaryOperator(OperatorType.GE, ColumnType.BOOLEAN, 
        //             ExpressionFactory.createScalarFunc(ColumnType.DOUBLE, ScalarFuncType.Distance, 
        //                 ImmutableList.of(
        //                     ExpressionFactory.createInputRef(manager.getPublishedSchema("geom")),
        //                     ExpressionFactory.createLiteral(ColumnType.POINT, new Point(114.0, 514.0))
        //                 )
        //             ),
        //             ExpressionFactory.createLiteral(ColumnType.DOUBLE, 90)
        //         )
        // ));
        // String sqlCmd = adapter.testGenerateSQL(plan);
        // assertEquals("SELECT name from street where ST_Distance(geom, ST_GeomFromText('POINT(114, 514)', 4326)) >= 90", sqlCmd);
    }
}
