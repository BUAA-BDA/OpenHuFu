package com.hufudb.onedb.owner.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Test;
import com.google.gson.Gson;

public class OwnerConfigFileTest {
  @Test
  public void testGenerateConfig() throws IOException {
    String source = "config_ci.json";
    if (System.getenv("ONEDB_TEST_LOCAL") != null) {
      source = "config.json";
    }
    URL ownerConfigPath = OwnerConfigFileTest.class.getClassLoader().getResource(source);
    Gson gson = new Gson();
    Reader reader = Files.newBufferedReader(Paths.get(ownerConfigPath.getPath()));
    OwnerConfigFile configFile = gson.fromJson(reader, OwnerConfigFile.class);
    OwnerConfig config = configFile.generateConfig();
    assertEquals(12345, config.port);
    assertEquals("localhost", config.hostname);
    assertFalse(config.useTLS);
    assertNotNull(config.threadPool);
    assertNotNull(config.acrossOwnerRpc);
    assertNotNull(config.tables);
    assertNotNull(config.adapter);
  }
}
