package com.hufudb.onedb.owner.adapter;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.assertNotNull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;

@RunWith(JUnit4.class)
public class AdapterLoaderTest {

  @Test
  public void loadAdapter() {
    String onedbRoot = System.getenv("ONEDB_ROOT");
    Path adapterDir = Paths.get(onedbRoot, "adapter");
    System.err.println(adapterDir.toString());
    Map<String, AdapterFactory> factoryMap = AdapterLoader.loadAdapters(adapterDir.toString());
    AdapterFactory factory = factoryMap.get("postgresql");
    AdapterConfig config = new AdapterConfig();
    config.catalog = "postgres";
    config.url = "jdbc:postgresql://postgres1:5432/postgres";
    config.user = "postgres";
    config.passwd = "onedb";
    Adapter adapter = factory.create(config);
    assertNotNull(adapter.getSchemaManager().getLocalTable("student"));
  }
}
