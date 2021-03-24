package tk.onedb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import tk.onedb.client.utils.OneDBLine;

public class OneDB {
  public static void main(String[] args) {
    final Options options = new Options();
    final Option model = new Option("m", "model", true, "model of fed");
    model.setRequired(true);
    options.addOption(model);
    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      Class.forName("tk.onedb.client.jdbc.OneDBDriver");
      cmd = parser.parse(options, args);
      final String m = cmd.getOptionValue("model", "model.json");
      List<String> dbargs = new ArrayList<>();
      dbargs.add("-u");
      dbargs.add("jdbc:onedb:model=" + m + ";lex=JAVA;caseSensitive=false;");
      dbargs.add("-n");
      dbargs.add("admin");
      dbargs.add("-p");
      dbargs.add("admin");
      OneDBLine.start(dbargs.toArray(new String[6]), null, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
