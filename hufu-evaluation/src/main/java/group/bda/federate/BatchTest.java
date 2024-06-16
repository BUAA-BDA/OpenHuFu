package group.bda.federate;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class BatchTest {

  private static final String JDBC_DRIVER = "group.bda.federate.sql.jdbc.HufuJDBCDriver";
  private static final String JDBC_URL = "jdbc:hufu:model=%s;lex=JAVA;caseSensitive=false;";
  private static final int REPEAT_TIMES = 4;
  private static final String OUTPUT_DIR = "output/";

  public static void main(String[] args) {
    final Options options = new Options();
    final Option model = new Option("m", "model", true, "model of fed");
    model.setRequired(true);
    final Option sql = new Option("s", "sql", true, "input of sql");
    sql.setRequired(true);
    final Option type = new Option("t", "type", true, "type of test: range_query, range_count, knn_query");
    type.setRequired(true);
    options.addOption(model);
    options.addOption(sql);
    options.addOption(type);
    final CommandLineParser parser = new DefaultParser();

    try {
      CommandLine cmd = parser.parse(options, args);;
      final String m = cmd.getOptionValue("model", "model.json");
      final String s = cmd.getOptionValue("sql", "sql");
      final String t = cmd.getOptionValue("type", "range_query");
      String[] splitS = s.split(File.separator);
      String dataset = splitS[splitS.length - 2];
      String resultDir = OUTPUT_DIR + dataset + "/result";
      String logDir = OUTPUT_DIR + dataset + "/logs";
      createDirectoryIfNotExists(OUTPUT_DIR);
      createDirectoryIfNotExists(resultDir);
      createDirectoryIfNotExists(logDir);

      Class.forName(JDBC_DRIVER);

      String jdbcUrl = String.format(JDBC_URL, m);
      try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
        Files.walk(Paths.get(s))
            .filter(Files::isRegularFile)
            .filter(f -> f.getFileName().toString().indexOf(t) != -1)
            .forEach(path -> executeSqlFile(conn, resultDir, logDir, path));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(0);
  }

  private static void executeSqlFile(Connection conn, String resultDir, String logDir, Path path) {
    String fileName = path.getFileName().toString();
    String resultFileName = resultDir + "/" + fileName.substring(0, fileName.lastIndexOf('.')) + ".csv";
    String logFileName = logDir + "/" + fileName.substring(0, fileName.lastIndexOf('.')) + ".log";

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