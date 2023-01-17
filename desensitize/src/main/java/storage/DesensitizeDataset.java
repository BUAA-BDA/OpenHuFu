package storage;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.ResultDataSet;
import factory.DesensitizeFactory;

import java.sql.ResultSet;
import java.util.List;

public class DesensitizeDataset extends ResultDataSet {

    public DesensitizeDataset(Schema schema, Schema desensitizationSchema, ResultSet result) {
        super(schema, desensitizationSchema, result);
    }

    @Override
    public DataSetIterator getIterator() {
        return new DesensitizeIterator(generateGetters());
    }

    protected class DesensitizeIterator extends ResultIterator {

        protected DesensitizeIterator(List<Getter> getters) {
            super(getters);
        }

        @Override
        public Object get(int columnIndex) {
            try {
                Object val = super.getters.get(columnIndex).get();
                if (desensitizationSchema != null && val != null) {
                    val =  DesensitizeFactory.implement(val, desensitizationSchema.getColumnDesc(columnIndex));
                }
                return val;
            } catch (Exception e) {
                LOG.error("Error in get of ResultDataSet: {}", e.getMessage());
                return null;
            }
        }
    }
}
