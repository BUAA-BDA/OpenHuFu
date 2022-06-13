package com.hufudb.onedb.owner.adapter.csv;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import org.junit.Test;
import com.hufudb.onedb.data.storage.DataSetIterator;

public class CsvDataSetTest {
  @Test
  public void testToString() throws IOException {
    URL source = CsvDataSetTest.class.getClassLoader().getResource("data/test1.csv");
    CsvDataSet dataset = new CsvDataSet(Paths.get(source.getPath()));
    DataSetIterator it = dataset.getIterator();
    int count = 0;
    while (it.next()) {
      for (int i = 0; i < it.size(); ++i) {
        it.get(i);
      }
      ++count;
    }
    assertEquals(9, count);
    dataset.close();
  }
}
