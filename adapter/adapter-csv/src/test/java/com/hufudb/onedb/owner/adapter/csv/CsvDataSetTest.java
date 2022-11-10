package com.hufudb.onedb.owner.adapter.csv;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Test;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class CsvDataSetTest {
  @Test
  public void testToString() throws IOException {
    URL source = CsvDataSetTest.class.getClassLoader().getResource("data/test1.csv");
    CSVFormat csvFormat = CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).setIgnoreSurroundingSpaces(true).setNullString("").build();
    Schema.Builder builder = Schema.newBuilder();
    CSVParser csvParser = CSVParser.parse(Paths.get(source.getPath()), StandardCharsets.UTF_8, csvFormat);
    csvParser.getHeaderNames().forEach(col -> builder.add(col, ColumnType.STRING));
    CsvDataSet dataset = new CsvDataSet(csvParser, builder.build());
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
