package group.bda.federate.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class CSVExporter {
  private CSVPrinter csvPrinter;

  public static boolean export(final List<String> headers, final Iterable<?> rows, final OutputStream outputStream) {
    try {
      final OutputStreamWriter writer = new OutputStreamWriter(outputStream);
      final CSVFormat csvFormat = CSVFormat.EXCEL.withHeader(headers.stream().toArray(String[]::new));
      final CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);
      csvPrinter.printRecords(rows);
      csvPrinter.flush();
      csvPrinter.close();
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public CSVExporter(final List<String> headers, final OutputStream outputStream) throws IOException {
    OutputStreamWriter writer = new OutputStreamWriter(outputStream);
    final CSVFormat csvFormat = CSVFormat.EXCEL.withHeader(headers.stream().toArray(String[]::new));
    this.csvPrinter = new CSVPrinter(writer, csvFormat);
  }

  public boolean addRow(final Iterable<?> values) {
    try {
      csvPrinter.printRecord(values);
    } catch (IOException e) {
      System.out.println("print csv row error");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean addRows(final Iterable<?> values) {
    try {
      csvPrinter.printRecords(values);
    } catch (IOException e) {
      System.out.println("print csv row error");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean close() {
    try {
      csvPrinter.flush();
      csvPrinter.close();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
