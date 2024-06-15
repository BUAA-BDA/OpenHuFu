package group.bda.federate;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

public class BatchTest {

  private static final String JDBC_DRIVER = "group.bda.federate.sql.jdbc.HufuJDBCDriver";
  private static final String JDBC_URL = "jdbc:hufu:model=%s;lex=JAVA;caseSensitive=false;";
  private static final int REPEAT_TIMES = 4;
  private static final String OUTPUT_DIR = "output";
  private static final String RESULT_DIR = OUTPUT_DIR + "/result";
  private static final String LOG_DIR = OUTPUT_DIR + "/logs";
  private static final String type = "osm";

  public static void main(String[] args) {
    try {
      createDirectoryIfNotExists(OUTPUT_DIR);
      createDirectoryIfNotExists(RESULT_DIR);
      createDirectoryIfNotExists(LOG_DIR);
      Class.forName(JDBC_DRIVER);
      String jdbcUrl = String.format(JDBC_URL, BatchTest.class.getClassLoader().getResource("model.json").getPath());
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        Files.walk(Paths.get(BatchTest.class.getClassLoader().getResource("sql").getPath()))
            .filter(Files::isRegularFile)
            .filter(path -> path.getFileName().toString().endsWith(type + ".sql"))
            .forEach(path -> executeSqlFile(conn, path));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(0);
  }

  private static void executeSqlFile(Connection conn, Path path) {
    String fileName = path.getFileName().toString();
    String resultFileName = RESULT_DIR + "/" + fileName.substring(0, fileName.lastIndexOf('.')) + ".csv";
    String logFileName = LOG_DIR + "/" + fileName.substring(0, fileName.lastIndexOf('.')) + ".log";

    try (BufferedReader reader = Files.newBufferedReader(path);
         PrintWriter writer = new PrintWriter(new FileWriter(resultFileName))) {
      StringBuilder header = new StringBuilder("File,SQL");
      for (int i = 1; i <= REPEAT_TIMES; i++) {
        header.append(",Execution ").append(i).append(" (ms)");
      }
      header.append(",Total Time (ms),Average Time (ms)");
      writer.println(header);

      String line;
      long totalExecutionTime = 0;
      int sqlCount = 0;
      while ((line = reader.readLine()) != null) {
        System.out.println("Executing SQL file: " + fileName + ", SQL: " + line);
        List<Long> times = new ArrayList<>();
        int i = 0;
        for (; i < REPEAT_TIMES; i++) {
          long startTime = System.nanoTime();
          try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(line);
            while (rs.next()) {
              // Fetch all results to consider the query as finished
            }
          }
          long endTime = System.nanoTime();
          times.add(endTime - startTime);
        }
        long totalSqlExecutionTime = times.stream().mapToLong(Long::longValue).sum();
        long averageSqlExecutionTime = totalSqlExecutionTime / REPEAT_TIMES;
        totalExecutionTime += totalSqlExecutionTime;
        sqlCount++;

        StringJoiner joiner = new StringJoiner(",");
        joiner.add(fileName);
        joiner.add("\"" + line + "\"");
        for (Long time : times) {
          double timeMs = time / 1_000_000.0;
          joiner.add(String.format("%.2f", timeMs));
        }
        joiner.add(String.valueOf(totalSqlExecutionTime / 1_000_000.0));
        joiner.add(String.valueOf(averageSqlExecutionTime / 1_000_000.0));
        writer.println(joiner);
      }
      writer.println(fileName + ",Total," + (totalExecutionTime / 1_000_000.0) + "," + (totalExecutionTime / sqlCount / REPEAT_TIMES / 1_000_000.0));
    } catch (IOException | SQLException e) {
      try (PrintWriter logWriter = new PrintWriter(new FileWriter(logFileName, true))) {
        e.printStackTrace(logWriter);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  private static void createDirectoryIfNotExists(String directory) throws IOException {
    Path path = Paths.get(directory);
    if (!Files.exists(path)) {
      Files.createDirectories(path);
    }
  }
}