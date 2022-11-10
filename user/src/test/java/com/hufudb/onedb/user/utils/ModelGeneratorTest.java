package com.hufudb.onedb.user.utils;

import static org.junit.Assert.assertNotNull;
import java.util.ArrayList;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.core.table.utils.PojoOwnerInfo;

public class ModelGeneratorTest {
  @Test
  public void testGenerateModel() {
    PojoOwnerInfo owner1 = new PojoOwnerInfo("owner1:123", "./cert/ca.pem");
    PojoOwnerInfo owner2 = new PojoOwnerInfo("owner2:123", "./cert/ca.pem");
    GlobalTableConfig gs1 = new GlobalTableConfig();
    gs1.tableName = "student";
    gs1.localTables = new ArrayList<>();
    gs1.localTables.add(new LocalTableConfig("owner1:123", "student1"));
    gs1.localTables.add(new LocalTableConfig("owner2:123", "student2"));
    GlobalTableConfig gs2 = new GlobalTableConfig();
    gs2.tableName = "taxi";
    gs2.localTables = new ArrayList<>();
    gs2.localTables.add(new LocalTableConfig("owner1:123", "taxi1"));
    gs2.localTables.add(new LocalTableConfig("owner2:123", "taxi2"));
    String model = ModelGenerator.generateModel(ImmutableList.of(owner1, owner2), ImmutableList.of(gs1, gs2));
    assertNotNull(model);
  }
}
