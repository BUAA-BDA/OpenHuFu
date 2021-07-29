package group.bda.federate.driver;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.GeoTools;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.data.Row;
import group.bda.federate.data.DataSet;
import group.bda.federate.data.StreamDataSet;
import group.bda.federate.rpc.FederateCommon;
import group.bda.federate.rpc.FederateCommon.FederateDataSet;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.Op;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateService.RangeCountResponse;
import group.bda.federate.driver.config.ServerConfig;
import group.bda.federate.driver.ir.GeomesaIRTranslator;
import group.bda.federate.driver.table.ServerTableInfo;
import group.bda.federate.driver.utils.AggCache;
import group.bda.federate.driver.utils.ComparableRow;
import group.bda.federate.driver.utils.DistanceDataSet;
import group.bda.federate.driver.utils.GeomesaConfig;
import group.bda.federate.driver.utils.GeomesaTypeConverter;
import io.grpc.ServerBuilder;

public class GeomesaServer extends FederateDBServer {
  private static class FederateGeomesaService extends FederateDBService {
    private static final Logger LOG = LogManager.getLogger(FederateGeomesaService.class);
    private DataStore dataStore;
    FilterFactory2 ff;

    FederateGeomesaService(GeomesaConfig config) {
      super(FedSpatialConfig.SERVER_THREAD_NUM);
      try {
        init(config);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void init(GeomesaConfig config) throws Exception {
      final Map<String, Serializable> params = new HashMap<String, Serializable>();
      params.put("hbase.catalog", config.catalog);
      params.put("hbase.zookeepers", config.zookeepers);
      ff = CommonFactoryFinder.getFilterFactory2(GeoTools.getDefaultHints());
      dataStore = DataStoreFinder.getDataStore(params);
      if (dataStore == null) {
        LOG.error("DataStore Init Failed!");
        System.out.println("DataStore Init Failed!");
        throw new RuntimeException("DataStore Init Failed!");
      }
      loadTables(config);
    }

    private void loadTables(ServerConfig config) {
      try {
        final String[] tableNames = dataStore.getTypeNames();
        for (final String tableName : tableNames) {
          ServerConfig.Table table = config.getTable(tableName);
          if (table == null) {
            continue;
          }
          final SimpleFeatureSource featureSource = dataStore.getFeatureSource(tableName);
          ServerTableInfo.IteratorBuilder tableInfoBuilder = ServerTableInfo.newBuilder();
          tableInfoBuilder.setTableName(tableName);
          if (featureSource != null) {
            final SimpleFeatureType featureType = featureSource.getSchema();
            final List<AttributeDescriptor> attributeDescriptors = featureType.getAttributeDescriptors();
            for (final AttributeDescriptor attributeDescriptor : attributeDescriptors) {
              String columnName = attributeDescriptor.getLocalName();
              Level level = table.getLevel(columnName);
              if (level.equals(Level.HIDE)) continue;
              String typeName = attributeDescriptor.getType().getBinding().getSimpleName();
              tableInfoBuilder.add(columnName, GeomesaTypeConverter.convert(typeName), level);
            }
            ServerTableInfo tableInfo = tableInfoBuilder.build();
            LOG.info("add {}", tableInfo.toString());
            tableInfoMap.put(tableName, tableInfo);
          } else {
            LOG.error("get table [{}] failed", tableName);
          }
        }
      } catch (final Exception e) {
        LOG.error("get tableNames failed!");
        throw new RuntimeException("get tableNames failed!", e);
      }
    }

    @Override
    public void fedSpatialQueryInternal(group.bda.federate.rpc.FederateService.Query request,
        StreamDataSet streamDataSet) throws SQLException {
      SimpleFeatureCollection features = getFeatures(request);
      List<Integer> projects = new ArrayList<>();
      // String[] projectStrs = request.getProjectExpList().stream().map(e ->
      //   new GeomesaIRTranslator(e, tableHeader).translate()).toArray(String[]::new);
      for (Expression exp : request.getProjectExpList()) {
        List<IR> irs = exp.getIrList();
        IR ir = irs.get(irs.size() - 1);
        if (ir.getOp().equals(Op.kAggFunc) && ir.getFunc().equals(Func.kCount)) {
          break;
        }
        projects.add(ir.getIn(0).getRef());
      }
      int fetch = request.getFetch();
      if (request.hasAggUuid()) {
        String aggUuid = request.getAggUuid();
        fillAggregateDataSet(features.features(), aggUuid, streamDataSet);
      } else {
        fillDataSet(features.features(), projects, streamDataSet, fetch);
      }
    }

    @Override
    public DistanceDataSet calKnn(group.bda.federate.rpc.FederateService.Query query) throws SQLException {
      String tableName = query.getTableName();
      Header tableHeader = getTableHeader(tableName);
      List<Integer> projects = new ArrayList<>();
      for (Expression exp : query.getProjectExpList()) {
        List<IR> irs = exp.getIrList();
        IR ir = irs.get(irs.size() - 1);
        projects.add(ir.getIn(0).getRef());
      }
      for (Expression exp : query.getFilterExpList()) {
        GeomesaIRTranslator translator = new GeomesaIRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn != null) {
          int k = translator.getKOfkNN(knn);
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          // filters.add(translator.translate())
          SimpleFeatureCollection col = calKnn(tableName, p.getX(), p.getY(), k);
          return fillDistanceDataSet(Header.fromProto(query.getHeader()), col.features(), projects, p.getX(), p.getY(), k);
        }
      }
      return null;
    };

    private SimpleFeatureCollection getFeatures(group.bda.federate.rpc.FederateService.Query request) {
      String tableName = request.getTableName();
      Header outputHeader = Header.fromProto(request.getHeader());
      Header tableHeader = getTableHeader(tableName);
      List<String> filters = new ArrayList<>();
      for (Expression exp : request.getFilterExpList()) {
        GeomesaIRTranslator translator = new GeomesaIRTranslator(exp, tableHeader);
        IR knn = translator.getKNN();
        if (knn == null) {
          filters.add(translator.translate());
        } else {
          int k = translator.getKOfkNN(knn);
          group.bda.federate.sql.type.Point p = translator.getPointOfkNN(knn);
          return calKnn(tableName, p.getX(), p.getY(), k);
        }
      }
      try {
        Filter filter;
        if (filters.isEmpty()) {
          filter = Filter.INCLUDE;
        } else {
          filter = ECQL.toFilter(String.join(" AND ", filters), ff);
        }
        Query query = new Query(tableName, filter);
        return dataStore.getFeatureSource(tableName).getFeatures(query);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    private void fillDataSet(SimpleFeatureIterator features, List<Integer> projects, StreamDataSet dataSet, int fetch) {
      final Header header = dataSet.getHeader();
      final int columnSize = header.size();
      for (int j = 0; features.hasNext() && j < fetch; ++j) {
        SimpleFeature feature = features.next();
        Row.RowBuilder builder = Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          if (header.getLevel(i).equals(Level.HIDE)) {
            continue;
          }
          Object o = feature.getAttribute(projects.get(i));
          switch (header.getType(i)) {
            case BYTE:
              builder.set(i, ((Number) o).byteValue());
              break;
            case SHORT:
              builder.set(i, ((Number) o).shortValue());
              break;
            case INT:
              builder.set(i, ((Number) o).intValue());
              break;
            case LONG:
              builder.set(i, ((Number) o).longValue());
              break;
            case FLOAT:
              builder.set(i, ((Number) o).floatValue());
              break;
            case DOUBLE:
              builder.set(i, ((Number) o).doubleValue());
              break;
            case STRING:
              builder.set(i, o.toString());
              break;
            case BOOLEAN:
              builder.set(i, (Boolean) o);
              break;
            case DATE:
              // Divide by 86400000L to prune time field
              Long date = ((Date) o).getTime() / 86400000L;
              builder.set(i, date);
              break;
            case TIME:
            case TIMESTAMP:
              if (o instanceof java.util.Date) {
                builder.set(i, ((java.util.Date) o).getTime());
              } else {
                builder.set(i, ((Timestamp) o).getTime());
              }
              break;
            case POINT:
              org.locationtech.jts.geom.Point p = (org.locationtech.jts.geom.Point) o;
              builder.set(i, new group.bda.federate.sql.type.Point(p.getX(), p.getY()));
              break;
            default:
              break;
          }
        }
        dataSet.addRow(builder.build());
      }
    }

    private void fillAggregateDataSet(SimpleFeatureIterator features, String aggUuid, StreamDataSet dataSet) {
      final Header header = dataSet.getHeader();
      int columnSize = header.size();
      Row.RowBuilder builder = Row.newBuilder(columnSize);
      Row.RowBuilder rawBuilder = Row.newBuilder(columnSize);
      int count = 0;
      while (features.hasNext()) {
        count++;
        features.next();
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

    private DistanceDataSet fillDistanceDataSet(Header header, SimpleFeatureIterator features, List<Integer> projects, double lon, double lat, int fetch) {
      final int columnSize = header.size();
      List<ComparableRow<Double>> comparableRows = new ArrayList<>();
      for (int j = 0; features.hasNext() && j < fetch; ++j) {
        SimpleFeature feature = features.next();
        Row.RowBuilder builder = Row.newBuilder(columnSize);
        for (int i = 0; i < columnSize; ++i) {
          if (header.getLevel(i).equals(Level.HIDE)) {
            continue;
          }
          Object o = feature.getAttribute(projects.get(i));
          switch (header.getType(i)) {
            case BYTE:
              builder.set(i, ((Number) o).byteValue());
              break;
            case SHORT:
              builder.set(i, ((Number) o).shortValue());
              break;
            case INT:
              builder.set(i, ((Number) o).intValue());
              break;
            case LONG:
              builder.set(i, ((Number) o).longValue());
              break;
            case FLOAT:
              builder.set(i, ((Number) o).floatValue());
              break;
            case DOUBLE:
              builder.set(i, ((Number) o).doubleValue());
              break;
            case STRING:
              builder.set(i, o.toString());
              break;
            case BOOLEAN:
              builder.set(i, (Boolean) o);
              break;
            case DATE:
              // Divide by 86400000L to prune time field
              Long date = ((Date) o).getTime() / 86400000L;
              builder.set(i, date);
              break;
            case TIME:
            case TIMESTAMP:
              if (o instanceof java.util.Date) {
                builder.set(i, ((java.util.Date) o).getTime());
              } else {
                builder.set(i, ((Timestamp) o).getTime());
              }
              break;
            case POINT:
              org.locationtech.jts.geom.Point p = (org.locationtech.jts.geom.Point) o;
              builder.set(i, new group.bda.federate.sql.type.Point(p.getX(), p.getY()));
              break;
            default:
              break;
          }
        }
        Point point = (Point) feature.getDefaultGeometry();
        double distance = Math.sqrt(Math.pow(point.getX() - lon, 2) + Math.pow(point.getY() - lat, 2));
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

    public SimpleFeatureCollection calKnn(final String tableName, final double lon, final double lat,
        final int k) {
      try {
        DefaultFeatureCollection inputCollection = new DefaultFeatureCollection();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(SimpleFeatureTypes.createType("knn", "*geom:Point:srid=4326"));
        builder.set("geom", String.format("POINT (%f %f)", lon, lat));
        inputCollection.add(builder.buildFeature(null));
        final SimpleFeatureCollection dataFeatures = dataStore.getFeatureSource(tableName).getFeatures();
        final KNearestNeighborSearchProcess knn = new KNearestNeighborSearchProcess();
        double rangeRadius = 100000.0;
        SimpleFeatureCollection result = knn.execute(inputCollection, dataFeatures, k, rangeRadius, Double.MAX_VALUE);
        return result;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error creating knn query:", e);
      }
    }
  }
  
  public GeomesaServer(GeomesaConfig config) throws IOException {
    super(ServerBuilder.forPort(config.port), config.port, new FederateGeomesaService(config));
  }

  public static void main(final String[] args) throws Exception {
    final Options options = new Options();
    Option config = new Option("c", "config", true, "geomesa config");
    config.setRequired(true);
    options.addOption(config);

    final CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    Gson gson = new Gson();

    try {
      cmd = parser.parse(options, args);
      Reader reader = Files.newBufferedReader(Paths.get(cmd.getOptionValue("config")));
      GeomesaConfig gConfig = gson.fromJson(reader, GeomesaConfig.class);
      final GeomesaServer server = new GeomesaServer(gConfig);
      server.start();
      server.blockUntilShutdown();
    } catch (final ParseException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}
