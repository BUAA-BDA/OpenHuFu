package group.bda.federate.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;

public class CSVReader {
  private CSVParser csvParser;
  private File csvFile;

  public CSVReader(String path) throws IOException {
    csvFile = new File(path);
    csvParser = CSVParser.parse(csvFile, Charset.forName("utf8"), CSVFormat.RFC4180.withHeader());
  }

  public DataSet load(Header header) throws Exception {
    int size = header.size();
    DataSet dataSet = DataSet.newDataSet(header);
    try {
      for (CSVRecord record : csvParser) {
        DataSet.DataRowBuilder builder = dataSet.newRow();
        for (int i = 0; i < size; ++i) {
          builder.setString(i, record.get(i).trim());
        }
        builder.build();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return dataSet;
  }
}
