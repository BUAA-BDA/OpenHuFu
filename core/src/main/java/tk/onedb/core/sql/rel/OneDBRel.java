package tk.onedb.core.sql.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface OneDBRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  class Implementor {
  }
}
