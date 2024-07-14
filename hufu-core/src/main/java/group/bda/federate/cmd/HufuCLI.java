package group.bda.federate.cmd;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import group.bda.federate.cmd.utils.HufuLine;

public class HufuCLI {
  public static void main(String[] args) {
    final Options options = new Options();
    final Option model = new Option("m", "model", true, "model of fed");
    model.setRequired(true);
    options.addOption(model);
    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      Class.forName("group.bda.federate.sql.jdbc.HufuJDBCDriver");
      cmd = parser.parse(options, args);
      final String m = cmd.getOptionValue("model", "model.json");
      List<String> fedArgs = new ArrayList<>();
      fedArgs.add("-u");
      fedArgs.add("jdbc:hufu:model=" + m + ";lex=JAVA;caseSensitive=false;");
      fedArgs.add("-n");
      fedArgs.add("admin");
      fedArgs.add("-p");
      fedArgs.add("admin");
      HufuLine.start(fedArgs.toArray(new String[6]), System.in, true);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
}
