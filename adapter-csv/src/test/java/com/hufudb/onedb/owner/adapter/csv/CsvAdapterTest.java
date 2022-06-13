package com.hufudb.onedb.owner.adapter.csv;

import java.net.URL;
import org.junit.Test;

public class CsvAdapterTest {
  @Test
  public void testLoadTables() {
    URL source = CsvAdapterTest.class.getClassLoader().getResource("data");
    CsvAdapter adapter = new CsvAdapter(source.getPath());
    adapter.shutdown();
  }
}
