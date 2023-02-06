package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuCalc;
import com.hufudb.openhufu.core.sql.rel.OpenHuFuRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class OpenHuFuCalcRule extends ConverterRule {
  static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(LogicalCalc.class, Convention.NONE, OpenHuFuRel.CONVENTION, "OpenHuFuCalcRule")
          .withRuleFactory(OpenHuFuCalcRule::new);

  protected OpenHuFuCalcRule(Config config) {
    super(config);
  }

  public RelNode convert(RelNode relNode) {
    final LogicalCalc calc = (LogicalCalc) relNode;
    final RelNode input = calc.getInput();
    return OpenHuFuCalc.create(
        convert(input, input.getTraitSet().replace(OpenHuFuRel.CONVENTION)), calc.getProgram());
  }
}
