package com.hufudb.openhufu.owner.adapter.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import com.hufudb.openhufu.data.function.Mapper;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.EmptyDataSet;
import com.hufudb.openhufu.data.storage.MapDataSet;
import com.hufudb.openhufu.data.storage.Point;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.apache.commons.csv.CSVRecord;

public class CsvTable {

  final Path dataPath;
  final Path schemaPath;
  final CSVFormat csvFormat;
  final String tableName;
  final Schema schema;
  final String delimiter;

  CsvTable(String tableName, Path schemaPath, Path dataPath, String delimiter) throws IOException {
    this.csvFormat =
        CSVFormat.RFC4180.builder().setDelimiter(delimiter).setHeader().setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true).setNullString("").build();
    this.delimiter = delimiter;
    this.dataPath = dataPath;
    this.schemaPath = schemaPath;
    this.tableName = tableName;
    this.schema = parseSchema(schemaPath);
  }

  Schema parseSchema(Path schemaPath) throws IOException {
    CSVFormat schemaFormat =
        CSVFormat.RFC4180.builder().setDelimiter(delimiter).setHeader().setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true).setNullString("").build();
    Schema.Builder builder = Schema.newBuilder();
    CSVParser csvParser = CSVParser.parse(schemaPath, StandardCharsets.UTF_8, schemaFormat);
    CSVRecord record = csvParser.getRecords().get(0);
    List<String> headers = csvParser.getHeaderNames();
    for (int i = 0; i < headers.size(); i++) {
      builder.add(headers.get(i), ColumnType.valueOf(record.get(i)));
    }
    csvParser.close();
    return builder.build();
  }

  Schema getSchema() {
    return schema;
  }

  DataSet scanWithSchema(Schema outSchema, List<Integer> mapping) {
    List<Mapper> mappers = new ArrayList<>();
    for (int i = 0; i < mapping.size(); ++i) {
      final int actualColumnIdx = mapping.get(i);
      final ColumnType outType = outSchema.getType(i);
      switch (outType) {
        case BOOLEAN:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Boolean.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case BYTE:
        case SHORT:
        case INT:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Integer.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case LONG:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Long.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case DATE:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Date.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case TIME:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Time.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case TIMESTAMP:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Timestamp.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case FLOAT:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Float.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case DOUBLE:
          mappers.add(row -> {
            String input = (String) row.get(actualColumnIdx);
            if (input == null) {
              return null;
            } else {
              return Double.valueOf((String) row.get(actualColumnIdx));
            }
          });
          break;
        case POINT:
          mappers.add(row -> Point.parse((String) row.get(actualColumnIdx)));
          break;
        case STRING:
          mappers.add(row -> row.get(actualColumnIdx));
          break;
        default:
          throw new RuntimeException("Unsupport type for csv adapter");
      }
    }
    try {
      CSVParser csvParser = CSVParser.parse(dataPath, StandardCharsets.UTF_8, csvFormat);
      return MapDataSet.create(outSchema, mappers, new CsvDataSet(csvParser, schema));
    } catch (IOException e) {
      return EmptyDataSet.INSTANCE;
    }
  }
}
