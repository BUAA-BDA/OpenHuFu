package com.hufudb.onedb.data.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.hufudb.onedb.proto.OneDBData.TableSchemaListProto;
import com.hufudb.onedb.proto.OneDBData.TableSchemaProto;
import com.hufudb.onedb.proto.OneDBData.SchemaProto;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class SchemaTest {

    List<ColumnDesc> generateColumnDescs(int size, boolean setModifier) {
        List<ColumnDesc> columns = new ArrayList<>();
        ColumnType[] coltypes = ColumnType.values();
        Modifier[] modifiers = Modifier.values();
        for (int i = 0; i < size; i++) {
            ColumnType coltype = coltypes[i % (modifiers.length - 1)];//except UNRECOGNIZED
            Modifier modifier = Modifier.HIDDEN;
            if (setModifier) modifier = modifiers[i % (modifiers.length - 1)];//except UNRECOGNIZED
            ColumnDesc col = ColumnDesc.newBuilder().setName(String.format("%s_%s_%s", i, coltype.toString(), modifier.toString())).setType(coltype).setModifier(modifier).build();
            columns.add(col);
        }
        return columns;
    }

    void compareSchemaProto(Schema schema, List<ColumnDesc> columns) {
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(schema.getName(i), columns.get(i).getName());
            assertEquals(schema.getType(i), columns.get(i).getType());
            assertEquals(schema.getModifier(i), columns.get(i).getModifier());
        }
    }

    @Test
    public void testSchema() {
        List<ColumnDesc> columns = generateColumnDescs(13, true);
        Schema schema1 = new Schema(columns);
        Schema.Builder builder = new Schema.Builder();
        for (ColumnDesc colDes: columns) builder.add(colDes);
        Schema schema2 = builder.build();
        assertEquals(builder.size(), columns.size());
        compareSchemaProto(schema1, columns);
        compareSchemaProto(schema2, columns);
        assertEquals(schema1, schema2);

        //test merge
        List<ColumnDesc> addcolumns = generateColumnDescs(20, false);
        Schema.Builder addbuilder = new Schema.Builder();
        for (ColumnDesc colDes: addcolumns) addbuilder.add(colDes.getName(), colDes.getType());
        Schema addSchema = addbuilder.build();
        builder.merge(addSchema);
        assertEquals(builder.size(), columns.size() + addcolumns.size());
        Schema schema3 = builder.build();
        columns.addAll(addcolumns);
        assertNotEquals(schema3, schema2);
        compareSchemaProto(schema3, columns);

        //test toString
        String ans = String.join("|", columns.stream().map(col-> String.format("%s:%s:%s", col.getName(), col.getType().toString(), col.getModifier().toString())).collect(Collectors.toList()));
        assertEquals(schema3.toString(), ans);
    }

    void compareTableSchemaProto(TableSchema tableSchema, TableSchemaProto tableSchemaProto) {
        assertEquals(tableSchema.getName(), tableSchemaProto.getName());
        List<ColumnDesc> columns = tableSchemaProto.getSchema().getColumnDescList();
        assertEquals(tableSchema.size(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(tableSchema.getColumnIndex(columns.get(i).getName()), i);
        }
        assertEquals(tableSchema.toProto(), tableSchemaProto);
        //test toString
        String ans = String.format("[%s](%s)", tableSchemaProto.getName(), new Schema(tableSchemaProto.getSchema()).toString());
        assertEquals(tableSchema.toString(), ans);
    }

    TableSchemaListProto generateTableSchemaListProto(int size) {
        List<TableSchemaProto> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            List<ColumnDesc> columns = generateColumnDescs(20, true);
            SchemaProto schemaProto = SchemaProto.newBuilder().addAllColumnDesc(columns).build();
            TableSchemaProto tableSchemaProto = TableSchemaProto.newBuilder().setName("table" + i).setSchema(schemaProto).build();
            list.add(tableSchemaProto);
        }
        return TableSchemaListProto.newBuilder().addAllTable(list).build();
    }

    @Test
    public void testTableSchema() {
        TableSchema tableSchema1 = TableSchema.of("name_only");
        assertEquals(tableSchema1.getName(), "name_only");
        assertEquals(tableSchema1.getSchema(), Schema.EMPTY);
        tableSchema1 = TableSchema.fromName("name_only");
        assertEquals(tableSchema1.getName(), "name_only");
        assertEquals(tableSchema1.getSchema(), Schema.EMPTY);

        //single
        List<ColumnDesc> columns = generateColumnDescs(30, true);
        SchemaProto schemaProto = SchemaProto.newBuilder().addAllColumnDesc(columns).build();
        TableSchemaProto tableSchemaProto = TableSchemaProto.newBuilder().setName("table").setSchema(schemaProto).build();
        Map<String, Integer> colIndex = new HashMap<String, Integer>();
        for (int i = 0; i < columns.size(); i++) {
            colIndex.put(columns.get(i).getName(), i);
        }

        TableSchema tableSchema2 = TableSchema.of("table", columns);
        compareTableSchemaProto(tableSchema2, tableSchemaProto);
        TableSchema tableSchema3 = TableSchema.of("table", new Schema(columns));
        compareTableSchemaProto(tableSchema3, tableSchemaProto);
        TableSchema tableSchema4 = new TableSchema("table", new Schema(columns), colIndex);
        compareTableSchemaProto(tableSchema4, tableSchemaProto);
        TableSchema tableSchema5 = TableSchema.fromProto(tableSchemaProto);
        compareTableSchemaProto(tableSchema5, tableSchemaProto);
        TableSchema.Builder builder = TableSchema.newBuilder();
        for (ColumnDesc colDes: columns) builder.add(colDes.getName(), colDes.getType(), colDes.getModifier());
        builder.setTableName("table");
        TableSchema tableSchema6 = builder.build();
        compareTableSchemaProto(tableSchema6, tableSchemaProto);

        //multiple
        TableSchemaListProto tableSchemaListProto = generateTableSchemaListProto(10);
        List<TableSchema> tables = TableSchema.fromProto(tableSchemaListProto);
        List<TableSchemaProto> tableProtos = tableSchemaListProto.getTableList();
        assertEquals(tables.size(), tableProtos.size());
        for (int i = 0; i < tableProtos.size(); i++) compareTableSchemaProto(tables.get(i), tableProtos.get(i));
    }

    @Test
    public void testReaptedColumnName() {
        TableSchema.Builder builder = TableSchema.newBuilder();
        builder.add("col1", ColumnType.INT);
        builder.add("col2", ColumnType.BLOB);
        builder.add("col3", ColumnType.LONG);
        try {
            builder.add("col2", ColumnType.BOOLEAN);
            fail("TableSchema.Builder allows reapted column name!");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "column col2 already exist");
        }

        try {
            builder.add("col1", ColumnType.INT, Modifier.PUBLIC);
            fail("TableSchema.Builder allows reapted column name!");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "column col1 already exist");
        }
    }
}
