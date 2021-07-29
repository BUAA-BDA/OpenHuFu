package group.bda.federate.driver;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.operations.KNN;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.data.Row;
import group.bda.federate.data.StreamDataSet;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.Op;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateCommon.FederateDataSet;
import group.bda.federate.rpc.FederateService.Query;
import group.bda.federate.driver.ir.IRTranslator;
import group.bda.federate.driver.table.ServerTableInfo;
import group.bda.federate.driver.utils.ComparableRow;
import group.bda.federate.driver.utils.DistanceDataSet;
import group.bda.federate.driver.utils.SpatialhadoopConfig;
import group.bda.federate.driver.utils.AggCache;
import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.sql.type.Point;
import io.grpc.ServerBuilder;

public class SpatialhadoopServer extends FederateDBServer {
  private static class SpatialhadoopService extends FederateDBService {
    private static final Logger LOG = LogManager.getLogger(SpatialhadoopService.class);

    private String pointPath;
    private String indexPath;

    SpatialhadoopService(SpatialhadoopConfig config) {
      super();
      System.setProperty("hadoop.home.dir", config.hadoophome);
      this.pointPath = config.sourceFile;
      File tmp = new File(this.pointPath);
      String fileName = tmp.getName().split("\\.")[0];
      this.indexPath = fileName + ".rtree";
      File file = new File(this.indexPath);
      if (!file.exists()) {
        try {
          Indexer.main(new String[] { this.pointPath, this.indexPath, "sindex:rtree", "shape:point" });
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      try {
        ServerTableInfo.IteratorBuilder builder = ServerTableInfo.newBuilder();
        builder.setTableName(config.tableName);
        builder.add("sid", FederateFieldType.LONG, Level.PROTECTED);
        builder.add("geom", FederateFieldType.POINT, Level.PRIVATE);
        tableInfoMap.put(config.tableName, builder.build());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private static double getDistance(double lng1, double lat1, double lng2, double lat2) {
      return Math.sqrt(Math.pow(lng1 - lng2, 2) + Math.pow(lat1 - lat2, 2));
    }

    private List<String> rangeQuerySpatialhadoop(double lon, double lat, double radius)
        throws IOException, ClassNotFoundException, InterruptedException, RuntimeException {
      double x1 = lon - radius;
      double y1 = lat - radius;
      double x2 = lon + radius;
      double y2 = lat + radius;
      String range = Double.toString(x1) + "," + Double.toString(y1) + "," + Double.toString(x2) + ","
          + Double.toString(y2);
      RangeQuery.main(new String[] { this.indexPath, "./result_rangeQuery", "rect:" + range, "shape:point" });
      File file = new File("./result_rangeQuery");
      File file_crc = new File("./.result_rangeQuery.crc");
      List<String> res = FileUtils.readLines(file);
      if (!(file.delete() && file_crc.delete())) {
        throw new RuntimeException("delete file fail !");
      }
      return res;
    }

    private List<String> knnSpatialhadoop(double lon, double lat, int calk) throws Exception {
      String center = Double.toString(lon) + "," + Double.toString(lat);
      KNN.main(new String[] { this.indexPath, "./result_knn", "point:" + center, "k:" + Integer.toString(calk), "shape:point" });
      File file = new File("./result_knn");
      File file_crc = new File("./.result_knn.crc");
      List<String> res = FileUtils.readLines(file);
      if (!(file.delete() && file_crc.delete())) {
          throw new RuntimeException("delete file fail !");
      }
      return res;
    }

    private List<Point> getPoints(Query request) {
      Header tableHeader = getTableHeader(request.getTableName());
      for (Expression exp : request.getFilterExpList()) {
        IRTranslator translator = new IRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          int k = translator.getKOfkNN(knn);
          List<Point> res = calKnn(p.getX(), p.getY(), k);
          LOG.info("kNN query POINT({} {}) {} return {} rows", p.getX(), p.getY(), k, res.size());
          return res;
        }
        IR dwithin = translator.getDWithin();
        if (dwithin != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfDWithin(dwithin);
          double distance =  translator.getDisOfDWithin(dwithin);
          List<Point> res = rangeQuery(p.getX(), p.getY(), distance);
          LOG.info("range query POINT({} {}) {} return {} rows", p.getX(), p.getY(), distance, res.size());
          return res;
        }
      }
      return ImmutableList.of();
    }

    @Override
    public void fedSpatialQueryInternal(Query request, StreamDataSet streamDataSet) throws SQLException {
      List<Point> points = getPoints(request);
      List<Integer> projects = new ArrayList<>();
      for (Expression exp : request.getProjectExpList()) {
        List<IR> irs = exp.getIrList();
        IR ir = irs.get(irs.size() - 1);
        if (ir.getOp().equals(Op.kAggFunc) && ir.getFunc().equals(Func.kCount)) {
          break;
        }
        projects.add(ir.getIn(0).getRef());
      }
      if (request.hasAggUuid()) {
        fillAggregateDataSet(points, request.getAggUuid(), streamDataSet);
      } else {
        fillDataSet(points, projects, streamDataSet, request.getFetch());
      }
    }

    private void fillDataSet(List<Point> points, List<Integer> projects, StreamDataSet dataSet, int fetch) {
      final Header header = dataSet.getHeader();
      final int columnSize = header.size();
      for (int j = 0; j < points.size() && j < fetch; ++j) {
        Point p = points.get(j);
        group.bda.federate.data.Row.RowBuilder builder = group.bda.federate.data.Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          switch (header.getType(i)) {
            case LONG:
              builder.set(i, generateID(p.getX(), p.getY()));
              break;
            case POINT:
              builder.set(i, p);
              break;
            default:
              break;
          }
          dataSet.addRow(builder.build());
        }
      }
    }

    private void fillAggregateDataSet(List<Point> points, String aggUuid, StreamDataSet dataSet) {
      final Header header = dataSet.getHeader();
      int columnSize = header.size();
      group.bda.federate.data.Row.RowBuilder builder = group.bda.federate.data.Row.newBuilder(columnSize);
      group.bda.federate.data.Row.RowBuilder rawBuilder = group.bda.federate.data.Row.newBuilder(columnSize);
      int count = points.size();
      switch (header.getType(0)) {
        case INT:
          builder.set(0, 0);
          rawBuilder.set(0, count);
          break;
        case LONG:
          builder.set(0, 0);
          rawBuilder.set(0, (long) count);
          break;
        default:
          LOG.error("not support aggregate type");
          break;
      }
      dataSet.addRow(builder.build());
      buffer.set(aggUuid, new AggCache(rawBuilder.build(), header));
    }

    public List<Point> rangeQuery(final double lon, final double lat,
        final double radius) {
      List<String> res = new ArrayList<>();
      try {
        res = rangeQuerySpatialhadoop(lon, lat, radius);
      } catch (Exception e) {
        e.printStackTrace();
      }
      List<Point> points = new ArrayList<>();
      for (String pStr : res) {
        String[] tmp = pStr.split(",");
        double x = Double.parseDouble(tmp[0]);
        double y = Double.parseDouble(tmp[1]);
        if (getDistance(x, y, lon, lat) < radius) {
          points.add(new Point(x, y));
        }
      }
      return points;
    }

    @Override
    public DistanceDataSet calKnn(Query query) throws UnsupportedOperationException, SQLException {
      List<Expression> filters = query.getFilterExpList();
      Header tableHeader = getTableHeader(query.getTableName());
      for (Expression exp : filters) {
        IRTranslator translator = new IRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          int k = translator.getKOfkNN(knn);
          List<Point> rows = calKnn(p.getX(), p.getY(), k);
          LOG.info("kNN query POINT({} {}) {} return {} rows", p.getX(), p.getY(), k, rows.size());
          return fillDistanceDataSet(Header.fromProto(query.getHeader()), rows, p.getX(), p.getY());
        }
      }
      return null;
    }

    DistanceDataSet fillDistanceDataSet(Header header, List<Point> points, double lon, double lat) {
      List<ComparableRow<Double>> comparableRows = new ArrayList<>();
      for (Point p : points) {
        Row.RowBuilder builder = Row.newBuilder(1);
        double distance = Math.sqrt((lon - p.getX()) * (lon - p.getX()) + (lat - p.getY()) * (lat - p.getY()));
        builder.set(0, generateID(p.getX(), p.getY()));
        comparableRows.add(new ComparableRow<Double>(distance, builder.build()));
      }
      Collections.sort(comparableRows);
      List<Double> distances = new ArrayList<>();
      List<Row> rows = new ArrayList<>();
      for (ComparableRow<Double> cr : comparableRows) {
        distances.add(cr.compareKey);
        rows.add(cr.row);
      }
      return new DistanceDataSet(DataSet.newDataSetUnsafe(header, rows), distances);
    }

    public List<Point> calKnn(final double lon, final double lat,
        final int k) {
      final List<Point> results = new ArrayList<>();
      List<String> res = null;
      try {
        res = knnSpatialhadoop(lon, lat, k);
      } catch (Exception e) {
        throw new RuntimeException("Error creating knn query:", e);
      }
      for (String point : res) {
        String[] tmp = point.split(",");
        results.add(new Point(Double.parseDouble(tmp[0]), Double.parseDouble(tmp[1])));
      }
      return results;
    }

    public static long generateID(double x, double y) {
      return (((long)x) << 32) | (long)y;
    }
  }

  public SpatialhadoopServer(SpatialhadoopConfig config) throws IOException {
    super(ServerBuilder.forPort(config.port), config.port, new SpatialhadoopService(config));
  }

  public static void main(final String[] args) throws Exception {
    Options options = new Options();
    options.addRequiredOption("c", "config", true, "spaitalhadoop config path");
    CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      SpatialhadoopConfig sconfig = gson.fromJson(reader, SpatialhadoopConfig.class);
      SpatialhadoopServer server = new SpatialhadoopServer(sconfig);
      server.start();
      server.blockUntilShutdown();
    } catch (final ParseException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
