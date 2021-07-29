package group.bda.federate.sql.plan;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class FedSpatialRelRowCount extends RelMdRowCount {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, new FedSpatialRelRowCount());

  private FedSpatialRelRowCount() {
    super();
  }

  @Override
  public MetadataDef<BuiltInMetadata.RowCount> getDef() {
    return BuiltInMetadata.RowCount.DEF;
  }

  @Override
  public Double getRowCount(EnumerableLimit rel, RelMetadataQuery mq) {
    Double result = super.getRowCount(rel, mq);
    if (result != null) {
      return result + 1.0;
    } else {
      return null;
    }
  }
}
