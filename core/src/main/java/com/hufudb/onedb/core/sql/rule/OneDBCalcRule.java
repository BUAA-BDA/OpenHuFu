package com.hufudb.onedb.core.sql.rule;

import com.hufudb.onedb.core.sql.rel.OneDBCalc;
import com.hufudb.onedb.core.sql.rel.OneDBRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class OneDBCalcRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(LogicalCalc.class, Convention.NONE, OneDBRel.CONVENTION, "OneDBCalcRule")
          .withRuleFactory(OneDBCalcRule::new);

  protected OneDBCalcRule(Config config) {
    super(config);
  }

  public RelNode convert(RelNode relNode) {
    final LogicalCalc calc = (LogicalCalc) relNode;
    final RelNode input = calc.getInput();
    return OneDBCalc.create(
        convert(input, input.getTraitSet().replace(OneDBRel.CONVENTION)), calc.getProgram());
  }
}
