package group.bda.federate.driver;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;
import com.google.common.collect.ImmutableList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.simba.Dataset;
import org.apache.spark.sql.simba.SimbaSession;
import org.apache.spark.sql.simba.index.IndexType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.data.StreamDataSet;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.Op;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateCommon.FederateDataSet;
import group.bda.federate.rpc.FederateService.Query;
import group.bda.federate.driver.config.ServerConfig;
import group.bda.federate.driver.config.ServerConfig.Column;
import group.bda.federate.driver.table.ServerTableInfo;
import group.bda.federate.driver.utils.AggCache;
import group.bda.federate.driver.utils.ComparableRow;
import group.bda.federate.driver.utils.DistanceDataSet;
import group.bda.federate.driver.utils.SimbaConfig;
import group.bda.federate.driver.utils.SimbaIRTranslator;
import group.bda.federate.sql.type.FederateFieldType;
import io.grpc.ServerBuilder;

public class SimbaServer extends FederateDBServer {
  private static class FederateSimbaService extends FederateDBService {
    private static final Logger LOG = LogManager.getLogger(FederateSimbaService.class);

    private static SimbaSession simbaSession;
    private static Dataset<Row> sourceData;

    FederateSimbaService(SimbaConfig config) {
      super();
      try {
        simbaSession = SimbaSession.builder().master("local[*]").config("spark.executor.memory", "8g")
            .config("spark.driver.maxResultSize", "3g").config("spark.driver.memory", "8g")
            // .config("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
            // .config("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName())
            .appName("simbaServer").getOrCreate();
        init(config);
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }

    public void init(SimbaConfig config) throws Exception {
      try {
        List<StructField> fields = Arrays.asList(DataTypes.createStructField("sid", DataTypes.LongType, false),
            DataTypes.createStructField("lon", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false));
        StructType fedPointSchema = DataTypes.createStructType(fields);

        sourceData = simbaSession.simbaImplicits().dataframeToSimbaDataFrame(
            simbaSession.read().schema(fedPointSchema).format("csv").option("header", "false").load("file:///" + config.sourceFile).toDF());

        LOG.info("########  After Transform File: " + config.sourceFile + " ########");
        LOG.info("Read Total Rows: " + String.valueOf(sourceData.count()));
        sourceData.printSchema();

        LOG.info("########  Create-Persist-Load Simba Index  ########");
        final String[] dim = { "lon", "lat" };
        sourceData.index(IndexType.apply("rtree"), "simba-index", dim);
        sourceData.persistIndex("simba-index", config.indexFile);
        // simbaSession.sessionState().indexManager().loadIndex(simbaSession,
        // "simba-index", indexfile);

        LOG.info("########  Persist Source Data  ########");
        // StorageLevel sLevel = StorageLevel.MEMORY_ONLY_SER();
        StorageLevel sLevel = StorageLevel.MEMORY_AND_DISK_SER();
        sourceData.persist(sLevel);
      } catch (Exception e) {
        e.printStackTrace();
      }
      ServerTableInfo.IteratorBuilder builder = ServerTableInfo.newBuilder();
      builder.setTableName(config.tableName);
      builder.add("sid", FederateFieldType.LONG, Level.PROTECTED);
      builder.add("geom", FederateFieldType.POINT, Level.PRIVATE);
      tableInfoMap.put(config.tableName, builder.build());
    }

    @Override
    public void fedSpatialQueryInternal(Query request, StreamDataSet streamDataSet) throws SQLException {
      List<Row> rows = getRows(request);
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
        fillAggregateDataSet(rows, request.getAggUuid(), streamDataSet);
      } else {
        fillDataSet(rows, projects, streamDataSet, request.getFetch());
      }
    }

    @Override
    public DistanceDataSet calKnn(Query query) throws UnsupportedOperationException, SQLException {
      List<Expression> filters = query.getFilterExpList();
      Header tableHeader = getTableHeader(query.getTableName());
      for (Expression exp : filters) {
        SimbaIRTranslator translator = new SimbaIRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          int k = translator.getKOfkNN(knn);
          List<Row> rows = calKnn(p.getX(), p.getY(), k);
          LOG.info("kNN query POINT({} {}) {} return {} rows", p.getX(), p.getY(), k, rows.size());
          return fillDistanceDataSet(Header.fromProto(query.getHeader()), rows, ImmutableList.of(0), p.getX(), p.getY());
        }
      }
      return null;
    }

    private void fillDataSet(List<Row> rows, List<Integer> projects, StreamDataSet dataSet, int fetch) {
      final Header header = dataSet.getHeader();
      final int columnSize = header.size();
      for (int j = 0; j < rows.size() && j < fetch; ++j) {
        Row row = rows.get(j);
        group.bda.federate.data.Row.RowBuilder builder = group.bda.federate.data.Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          switch (header.getType(i)) {
            case LONG:
              Object o = row.get(projects.get(i));
              builder.set(i, ((Number) o).longValue());
              break;
            case POINT:
              double lo = ((Number)row.get(projects.get(i))).doubleValue();
              double la = ((Number)row.get(projects.get(i) + 1)).doubleValue();
              builder.set(i, new group.bda.federate.sql.type.Point(lo, la));
              break;
            default:
              break;
          }
          dataSet.addRow(builder.build());
        }
      }
    }

    private void fillAggregateDataSet(List<Row> rows, String aggUuid, StreamDataSet dataSet) {
      final Header header = dataSet.getHeader();
      int columnSize = header.size();
      group.bda.federate.data.Row.RowBuilder builder = group.bda.federate.data.Row.newBuilder(columnSize);
      group.bda.federate.data.Row.RowBuilder rawBuilder = group.bda.federate.data.Row.newBuilder(columnSize);
      int count = 0;
      for (Row row : rows) {
        count++;
      }
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

    DistanceDataSet fillDistanceDataSet(Header header, List<Row> rows, List<Integer> projects, double lon, double lat) {
      List<Double> distances = new ArrayList<>();
      DataSet dataSet = DataSet.newDataSet(header);
      final int columnSize = header.size();
      for (Row row : rows) {
        DataSet.DataRowBuilder builder = dataSet.newRow();
        for (int i = 0; i < columnSize; ++i) {
          switch (header.getType(i)) {
            case LONG:
              Object o = row.get(projects.get(i));
              builder.setUnsafe(i, ((Number) o).longValue());
              break;
            case POINT:
              double lo = ((Number)row.get(projects.get(i))).doubleValue();
              double la = ((Number)row.get(projects.get(i) + 1)).doubleValue();
              builder.setUnsafe(i, new group.bda.federate.sql.type.Point(lo, la));
              break;
            default:
              break;
          }
        }
        double lo = ((Number)row.get(1)).doubleValue();
        double la = ((Number)row.get(2)).doubleValue();
        double distance = Math.sqrt((lon - lo) * (lon - lo) + (lat - la) * (lat - la));
        distances.add(distance);
        builder.build();
      }
      return new DistanceDataSet(dataSet, distances);
    }

    public List<Row> getRows(Query request) {
      List<Expression> filters = request.getFilterExpList();
      Header tableHeader = getTableHeader(request.getTableName());
      for (Expression exp : filters) {
        SimbaIRTranslator translator = new SimbaIRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          int k = translator.getKOfkNN(knn);
          List<Row> res = calKnn(p.getX(), p.getY(), k);
          LOG.info("kNN query POINT({} {}) {} return {} rows", p.getX(), p.getY(), k, res.size());
          return res;
        }
        IR dwithin = translator.getDWithin();
        if (dwithin != null) {
          group.bda.federate.sql.type.Point p = translator.getPointOfDWithin(dwithin);
          double distance =  translator.getDisOfDWithin(dwithin);
          List<Row> res = rangeQuery(p.getX(), p.getY(), distance);
          LOG.info("range query POINT({} {}) {} return {} rows", p.getX(), p.getY(), distance, res.size());
          return res;
        }
      }
      return ImmutableList.of();
    }

    public List<Row> rangeQuery(final double lon, final double lat, final double radius)
        throws RuntimeException {
      try {
        final String[] dim = { "lon", "lat" };
        final double[] center = { lon, lat };
        return sourceData.circleRange(dim, center, radius).collectAsList();
      } catch (final Exception e) {
        e.printStackTrace();
        return ImmutableList.of();
      }
    }

    public List<Row> calKnn(final double lon, final double lat, final int k) {
      try {
        String[] dim = { "lon", "lat" };
        double lon1 = lon;
        double lat1 = lat;
        double[] p1 = { lon1, lat1 };
        return sourceData.knn(dim, p1, k).collectAsList();
      } catch (final Exception e) {
        e.printStackTrace();
        return ImmutableList.of();
      }
    }
  }

  public SimbaServer(SimbaConfig cfg) throws IOException {
    super(ServerBuilder.forPort(cfg.port), cfg.port, new FederateSimbaService(cfg));
  }

  public static void main(final String[] args) throws Exception {
    final Options options = new Options();
    options.addRequiredOption("c", "config", true, "simba config path");
    final CommandLineParser parser = new DefaultParser();
    Gson gson = new Gson();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      SimbaConfig cfg = gson.fromJson(reader, SimbaConfig.class);
      final SimbaServer server = new SimbaServer(cfg);
      server.start();
      server.blockUntilShutdown();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
