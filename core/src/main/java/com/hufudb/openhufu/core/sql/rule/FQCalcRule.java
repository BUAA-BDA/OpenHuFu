package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.FQCalc;
import com.hufudb.openhufu.core.sql.rel.FQRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class FQCalcRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(LogicalCalc.class, Convention.NONE, FQRel.CONVENTION, "OneDBCalcRule")
          .withRuleFactory(FQCalcRule::new);

  protected FQCalcRule(Config config) {
    super(config);
  }

  public RelNode convert(RelNode relNode) {
    final LogicalCalc calc = (LogicalCalc) relNode;
    final RelNode input = calc.getInput();
    return FQCalc.create(
        convert(input, input.getTraitSet().replace(FQRel.CONVENTION)), calc.getProgram());
  }
}
