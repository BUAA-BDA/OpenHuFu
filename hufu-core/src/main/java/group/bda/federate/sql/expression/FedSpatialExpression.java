package group.bda.federate.sql.expression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Geometries.Geom;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.ExpressionOrBuilder;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.IRField;
import group.bda.federate.rpc.FederateCommon.LiteralField;
import group.bda.federate.rpc.FederateCommon.Op;
import group.bda.federate.rpc.FederateCommon.Point;
import group.bda.federate.rpc.FederateCommon.RowField;
import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.sql.type.FederateTypeConverter;

public class FedSpatialExpression {
  List<IR> irs;
  IRField lastIRField;
  Header inputHeader;


  private FedSpatialExpression(Header inputHeader) {
    this.inputHeader = inputHeader;
    irs = new ArrayList<>();
  }

  private FedSpatialExpression(Header inputHeader, RexNode node) {
    this.inputHeader = inputHeader;
    IRBuilder builder = new IRBuilder(inputHeader, node);
    irs = builder.getIRs();
    lastIRField = builder.getLastIField();
  }

  private FedSpatialExpression(Header inputHeader, RexNode node, List<RexNode> localNodes) {
    this.inputHeader = inputHeader;
    IRBuilder builder = new IRBuilder(inputHeader, node, localNodes);
    irs = builder.getIRs();
    lastIRField = builder.getLastIField();
  }

  public FedSpatialExpression copy() {
    FedSpatialExpression exp = new FedSpatialExpression(inputHeader);
    exp.irs.addAll(irs);
    exp.lastIRField = lastIRField;
    return exp;
  }

  // for filter condition
  public static FedSpatialExpression create(Header inputHeader, RexNode node) {
    return new FedSpatialExpression(inputHeader, node);
  }

  // for calculate program
  public static List<FedSpatialExpression> create(Header inputHeader, List<Integer> projects,
                                                  List<RexNode> localNodes) {
    return projects.stream().map(i -> new FedSpatialExpression(inputHeader, localNodes.get(i), localNodes))
            .collect(Collectors.toList());
  }

  // for project
  public static List<FedSpatialExpression> create(Header inputHeader, List<RexNode> nodes) {
    return nodes.stream().map(node -> new FedSpatialExpression(inputHeader, node)).collect(Collectors.toList());
  }

  // create distancejoin filter template
  public static Expression createDistanceJoinFilterTemplate(int rightKey, double distance, boolean equal, FedSpatialExpressions rightExps) {
    FedSpatialExpression exp = rightExps.get(rightKey).copy();
    // distance
    RowField pointField = RowField.newBuilder().setP(Point.newBuilder().setLongitude(0).setLatitude(0).build()).build();
    LiteralField pointLiteral = LiteralField.newBuilder().setType(FederateFieldType.POINT.ordinal()).setValue(pointField).build();
    IRField point = IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(pointLiteral).build();
    RowField distanceField = RowField.newBuilder().setF64(distance).build();
    LiteralField distanceLiteral = LiteralField.newBuilder().setType(FederateFieldType.DOUBLE.ordinal()).setValue(distanceField).build();
    IRField distanceIR = IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(distanceLiteral).build();
    IR distanceIr = IR.newBuilder().addIn(point).addIn(exp.getLastIRField()).addIn(distanceIR).setOp(Op.kScalarFunc).setFunc(Func.kDWithin).setOutType(FederateFieldType.BOOLEAN.ordinal()).build();
    exp.addIR(distanceIr);
    return exp.toProto();
  }

  // fill distancejoin template
  public static Expression generateDistanceJoinFilter(String distanceFilter, group.bda.federate.sql.type.Point point) {
    Expression exp = str2Proto(distanceFilter);
    IR.Builder ir = exp.getIr(exp.getIrCount() - 1).toBuilder();
    RowField pointField = RowField.newBuilder().setP(Point.newBuilder().setLongitude(point.getX()).setLatitude(point.getY()).build()).build();
    LiteralField pointLiteral = LiteralField.newBuilder().setType(FederateFieldType.POINT.ordinal()).setValue(pointField).build();
    ir.setIn(0, IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(pointLiteral).build());
    return exp.toBuilder().setIr(exp.getIrCount() - 1, ir).build();
  }

  // for knn
  public static Expression createKNNJoinFilterTemplate(int rightKey, int k, FedSpatialExpressions rightExps) {
    FedSpatialExpression exp = rightExps.get(rightKey).copy();
    // knn
    RowField pointField = RowField.newBuilder().setP(Point.newBuilder().setLongitude(0).setLatitude(0).build()).build();
    LiteralField pointLiteral = LiteralField.newBuilder().setType(FederateFieldType.POINT.ordinal()).setValue(pointField).build();
    IRField point = IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(pointLiteral).build();
    RowField kField = RowField.newBuilder().setI32(k).build();
    LiteralField kLiteral = LiteralField.newBuilder().setType(FederateFieldType.INT.ordinal()).setValue(kField).build();
    IRField kIR = IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(kLiteral).build();
    IR knnIR = IR.newBuilder().addIn(point).addIn(exp.getLastIRField()).addIn(kIR).setOp(Op.kScalarFunc).setFunc(Func.kKNN).setOutType(FederateFieldType.BOOLEAN.ordinal()).build();
    exp.addIR(knnIR);
    return exp.toProto();
  }

  public static Expression generateKNNJoinFilter(String kNNFilter, group.bda.federate.sql.type.Point point) {
    Expression exp = str2Proto(kNNFilter);
    IR.Builder ir = exp.getIr(exp.getIrCount() - 1).toBuilder();
    RowField pointField = RowField.newBuilder().setP(Point.newBuilder().setLongitude(point.getX()).setLatitude(point.getY()).build()).build();
    LiteralField pointLiteral = LiteralField.newBuilder().setType(FederateFieldType.POINT.ordinal()).setValue(pointField).build();
    ir.setIn(0, IRField.newBuilder().setLevel(Level.PUBLIC.ordinal()).setLiteral(pointLiteral).build());
    return exp.toBuilder().setIr(exp.getIrCount() - 1, ir).build();
  }

  public static Header generateHeaderFromExpression(List<Expression> exps, List<Expression> filters, List<String> order, int limitCount) {
    Header.IteratorBuilder builder = Header.newBuilder();
    int idx = -1;
    // remove private columns
    Set<Integer> privateColumns = new HashSet<>();
    int privacyKnnColumn = locatePrivateKnn(exps, order, limitCount);
    privateColumns.add(privacyKnnColumn);
    for (Expression e : filters) {
      for (IR ir : e.getIrList()) {
        if (ir.hasFunc() && ir.getFunc() == Func.kKNN) {
          builder.setPrivacyKnn();
          break;
        }
      }
    }
    for (Expression e : exps) {
      idx++;
      if (privateColumns.contains(idx)) {
        builder.setPrivacy();
        if (idx == privacyKnnColumn) {
          builder.setPrivacyKnn();
        }
        FederateFieldType type = FederateFieldType.values()[e.getIr(e.getIrCount() - 1).getOutType()];
        builder.add("EXP$" + idx, type, Level.HIDE);
      } else if (isPrivacyAgg(e)) {
        builder.setPrivacyAgg();
        FederateFieldType type = FederateFieldType.values()[e.getIr(e.getIrCount() - 1).getOutType()];
        builder.add("EXP$" + idx, type, Level.HIDE);
      } else {
        FederateFieldType type = FederateFieldType.values()[e.getIr(e.getIrCount() - 1).getOutType()];
        builder.add("EXP$" + idx, type, Level.of(e.getLevel()));
      }
    }
    return builder.build();
  }

  private static boolean isPrivacyAgg(Expression expression) {
    IR ir = expression.getIr(expression.getIrCount() - 1);
    return expression.getLevel() != Level.PUBLIC.ordinal() && ir.getOp() == Op.kAggFunc
            && (ir.getFunc() == Func.kAvg || ir.getFunc() == Func.kCount || ir.getFunc() == Func.kSum);
  }

  public static int locatePrivateKnn(List<Expression> exps, List<String> order, int limitCount) {
    if (order.size() != 1 || limitCount == Integer.MAX_VALUE) {
      return -1;
    }
    int idx = Integer.parseInt(order.get(0).split(" ")[0]);
    String direction = order.get(0).split(" ")[1];
    if (!direction.equals("ASC")) {
      return -1;
    }
    Expression exp = exps.get(idx);
    IR ir = exp.getIr(exp.getIrCount() - 1);
    if (exp.getLevel() != Level.PRIVATE.ordinal() || ir.getOp() != Op.kScalarFunc || ir.getFunc() != Func.kDistance) {
      return -1;
    }
    return idx;
  }

  public static FedSpatialExpression createAggregate(Header inputHeader, AggregateCall call) {
    FedSpatialExpression exp = new FedSpatialExpression(inputHeader);
    int outType = IRBuilder.sqlType2FedTypeId(call.getType().getSqlTypeName());
    Func func = IRBuilder.getAggFunc(call.getAggregation().getKind());
    IR ir = IR.newBuilder().setOp(Op.kAggFunc).setFunc(func).setOutType(outType).build();
    exp.addIR(ir);
    int level;
    if (call.getAggregation().getKind() == SqlKind.SUM || call.getAggregation().getKind() == SqlKind.SUM0 || call.getAggregation().getKind() == SqlKind.COUNT) {
      level = (inputHeader.hasPrivacy() ? Level.PROTECTED : Level.PUBLIC).ordinal();
    } else {
      level = (inputHeader.hasPrivacy() ? Level.PRIVATE : Level.PUBLIC).ordinal();
    }
    exp.lastIRField = IRField.newBuilder().setLevel(level).setRef(inputHeader.size()).build();
    return exp;
  }

  public static List<FedSpatialExpression> createInputRef(Header inputHeader) {
    List<FedSpatialExpression> exps = new ArrayList<>();
    for (int i = 0; i < inputHeader.size(); ++i) {
      FedSpatialExpression exp = new FedSpatialExpression(inputHeader);
      int level = inputHeader.getLevel(i).ordinal();
      int type = inputHeader.getType(i).ordinal();
      IRField field = IRField.newBuilder().setRef(i).setLevel(level).build();
      IR ir = IR.newBuilder().setOp(Op.kAs).addIn(field).setOutType(type).build();
      exp.irs.add(ir);
      exp.lastIRField = IRField.newBuilder().setLevel(level).setRef(inputHeader.size()).build();
      exps.add(exp);
    }
    return exps;
  }

  void appendAggregate(Func func, int outType, int level) {
    IR ir = IR.newBuilder().setOp(Op.kAggFunc).setFunc(func).addIn(lastIRField).setOutType(outType).build();
    irs.add(ir);
    lastIRField = IRField.newBuilder().setRef(inputHeader.size() + irs.size() - 1).setLevel(level).build();
  }

  public Expression toProto() {
    return Expression.newBuilder().addAllIr(irs).setLevel(lastIRField.getLevel()).build();
  }

  public Level getLevel() {
    return Level.values()[lastIRField.getLevel()];
  }

  public FederateFieldType getType() {
    return FederateFieldType.values()[getLastIR().getOutType()];
  }

  public IR getLastIR() {
    return irs.get(irs.size() - 1);
  }

  public IRField getLastIRField() {
    return lastIRField;
  }

  private void addIR(IR ir) {
    this.irs.add(ir);
  }

  public void updateField(int irIndex, int inputIndex, IRField field) {
    IR ir = irs.get(irIndex);
    IR newIR = ir.toBuilder().setIn(inputIndex, field).build();
    irs.set(irIndex, newIR);
  }

  public static Expression str2Proto(String exp) {
    Expression.Builder builder = Expression.newBuilder();
    try {
      TextFormat.getParser().merge(exp, builder);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return builder.build();
  }

  public static List<Expression> str2Proto(List<String> exps) {
    List<Expression> result = new ArrayList<>();
    for (String exp : exps) {
      result.add(str2Proto(exp));
    }
    return result;
  }

  public String toString() {
    return Expression.newBuilder().setLevel(lastIRField.getLevel()).addAllIr(irs).build().toString();
  }

  static class IRBuilder {
    private List<IR> irs;
    private int count;
    private Header header;
    private IRField lastIRField;
    private List<RexNode> localNodes;

    IRBuilder(Header header, RexNode node) {
      this(header, node, ImmutableList.of(node));
    }

    IRBuilder(Header header, RexNode node, List<RexNode> localNodes) {
      this.count = header.size();
      this.irs = new ArrayList<>();
      this.header = header;
      this.localNodes = localNodes;
      this.lastIRField = buildIR(node);
    }

    static int sqlType2FedTypeId(SqlTypeName type) {
      return FederateTypeConverter.convert2FederateType(type).ordinal();
    }

    IRField buildIR(RexNode node) {
      switch (node.getKind()) {
        case LITERAL:
        case INPUT_REF:
          return addAs(node);
        default:
          return add(node);
      }
    }

    List<IR> getIRs() {
      return irs;
    }

    IRField getLastIField() {
      return lastIRField;
    }

    int generateIR(Op op, List<IRField> input, Func fun, int outType) {
      IR ir = IR.newBuilder().setOp(op).addAllIn(input).setFunc(fun).setOutType(outType).build();
      irs.add(ir);
      count++;
      return count - 1;
    }

    int generateIR(Op op, List<IRField> input, int outType) {
      IR ir = IR.newBuilder().setOp(op).addAllIn(input).setOutType(outType).build();
      irs.add(ir);
      count++;
      return count - 1;
    }

    IRField add(RexNode node) {
      switch (node.getKind()) {
        // binary
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case EQUALS:
        case NOT_EQUALS:
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
        case AND:
        case OR:
          return addBinary((RexCall) node);
        // unary
        case NOT:
        case PLUS_PREFIX:
        case MINUS_PREFIX:
          return addUnary((RexCall) node);
        // scalar function
        case OTHER_FUNCTION:
          return addScalarFunction((RexCall) node);
        // literal
        case LITERAL:
          return addLiteral((RexLiteral) node);
        case INPUT_REF:
          return addInputRef((RexInputRef) node);
        case LOCAL_REF:
          return addLocalRef((RexLocalRef) node);
        default:
          // aggregate function should not appear here
          throw new RuntimeException("can't translate " + node);
      }
    }

    IRField addAs(RexNode node) {
      int type = sqlType2FedTypeId(node.getType().getSqlTypeName());
      IRField irField = null;
      switch (node.getKind()) {
        case LITERAL:
          irField = addLiteral((RexLiteral) node);
          break;
        case INPUT_REF:
          irField = addInputRef((RexInputRef) node);
          break;
        default:
          throw new RuntimeException("can't translate " + node);
      }
      return newIRField(generateIR(Op.kAs, ImmutableList.of(irField), type), irField.getLevel());
    }

    IRField addBinary(RexCall call) {
      Op op = null;
      switch (call.getKind()) {
        case GREATER_THAN:
          op = Op.kGt;
          break;
        case GREATER_THAN_OR_EQUAL:
          op = Op.kGe;
          break;
        case LESS_THAN:
          op = Op.kLt;
          break;
        case LESS_THAN_OR_EQUAL:
          op = Op.kLe;
          break;
        case EQUALS:
          op = Op.kEq;
          break;
        case NOT_EQUALS:
          op = Op.kNe;
          break;
        case PLUS:
          op = Op.kPlus;
          break;
        case MINUS:
          op = Op.kMinus;
          break;
        case TIMES:
          op = Op.kTimes;
          break;
        case DIVIDE:
          op = Op.kDivide;
          break;
        case MOD:
          op = Op.kMod;
          break;
        case AND:
          op = Op.kAnd;
          break;
        case OR:
          op = Op.kOr;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<IRField> inputs = new ArrayList<>(2);
      IRField leftField = add(call.operands.get(0));
      IRField rightField = add(call.operands.get(1));
      inputs.add(leftField);
      inputs.add(rightField);
      int level = leftField.getLevel() > rightField.getLevel() ? leftField.getLevel() : rightField.getLevel();
      return newIRField(generateIR(op, inputs, sqlType2FedTypeId(call.getType().getSqlTypeName())), level);
    }

    IRField addUnary(RexCall call) {
      Op op = null;
      switch (call.getKind()) {
        case NOT:
          op = Op.kNot;
          break;
        case PLUS_PREFIX:
          op = Op.kPlus;
          break;
        case MINUS_PREFIX:
          op = Op.kMinus;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      IRField field = add(call.operands.get(0));
      int level = field.getLevel();
      List<IRField> input = ImmutableList.of(field);
      return newIRField(generateIR(op, input, sqlType2FedTypeId(call.getType().getSqlTypeName())), level);
    }

    IRField addScalarFunction(RexCall call) {
      Op op = Op.kScalarFunc;
      SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
      Func func = null;
      switch (function.getName()) {
        case "DWithin":
          func = Func.kDWithin;
          break;
        case "Distance":
          func = Func.kDistance;
          break;
        case "Point":
        case "MakePoint":
          func = Func.kPoint;
          break;
        case "KNN":
          func = Func.kKNN;
          break;
        default:
          throw new RuntimeException("can't translate function " + call);
      }
      return getInputAndGenerateIR(op, call.operands, func, sqlType2FedTypeId(call.getType().getSqlTypeName()));
    }

    IRField getInputAndGenerateIR(Op op, List<RexNode> operands, Func func, int outType) {
      List<IRField> inputs = new ArrayList<>();
      Level level = Level.PUBLIC;
      for (RexNode operand : operands) {
        IRField field = add(operand);
        inputs.add(field);
        if (field.getLevel() > level.ordinal()) {
          level = Level.of(field.getLevel());
        }
      }
      return newIRField(generateIR(op, inputs, func, outType), level);
    }

    static Func getAggFunc(SqlKind type) {
      switch (type) {
        case COUNT:
          return Func.kCount;
        case SUM:
        case SUM0:
          return Func.kSum;
        case AVG:
          return Func.kAvg;
        case MAX:
          return Func.kMax;
        case MIN:
          return Func.kMin;
        default:
          throw new RuntimeException("Unknown aggregate func " + type);
      }
    }

    // for literal
    static IRField addLiteral(RexLiteral node) {
      LiteralField field = getLiteral(node);
      return newIRField(field);
    }

    static LiteralField getLiteral(RexLiteral literal) {
      SqlTypeName sqlType = literal.getType().getSqlTypeName();
      FederateFieldType fedType = FederateTypeConverter.convert2FederateType(sqlType);
      RowField.Builder valueBuilder = RowField.newBuilder();
      Object o = literal.getValue();
      switch (sqlType) {
        case BOOLEAN:
          valueBuilder.setB((boolean) o);
          break;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
          valueBuilder.setI32(((BigDecimal) o).intValue());
          break;
        case BIGINT:
        case DATE:
        case TIME:
        case TIMESTAMP:
          valueBuilder.setI64(((BigDecimal) o).longValue());
          break;
        case FLOAT:
          valueBuilder.setF32(((BigDecimal) o).floatValue());
          break;
        case DOUBLE:
        case DECIMAL:
          valueBuilder.setF64(((BigDecimal) o).doubleValue());
          break;
        case GEOMETRY:
          com.esri.core.geometry.Point p = ((com.esri.core.geometry.Point) ((Geom) o).g());
          valueBuilder.setP(Point.newBuilder().setLongitude(p.getX()).setLatitude(p.getY()));
          break;
        case VARCHAR:
          valueBuilder.setStr(o.toString());
          break;
        default:
          throw new RuntimeException("Unknown literal type " + sqlType);
      }
      return LiteralField.newBuilder().setType(fedType.ordinal()).setValue(valueBuilder.build()).build();
    }

    static IRField newIRField(LiteralField literal) {
      return IRField.newBuilder().setLiteral(literal).setLevel(Level.PUBLIC.ordinal()).build();
    }

    // for ref
    IRField addInputRef(RexInputRef node) {
      int index = node.getIndex();
      return newIRField(index, header.getLevel(index));
    }

    IRField addLocalRef(RexLocalRef node) {
      RexNode local = localNodes.get(node.getIndex());
      switch (local.getKind()) {
        case INPUT_REF:
          return addInputRef((RexInputRef) local);
        case LITERAL:
          return addLiteral((RexLiteral) local);
        default:
          return add(local);
      }
    }

    IRField newIRField(int ref, Level level) {
      return IRField.newBuilder().setRef(ref).setLevel(level.ordinal()).build();
    }

    IRField newIRField(int ref, int level) {
      return IRField.newBuilder().setRef(ref).setLevel(level).build();
    }

    static FederateFieldType getOutType(ExpressionOrBuilder e) {
      int type = e.getIr(e.getIrCount() - 1).getOutType();
      return FederateFieldType.values()[type];
    }
  }
}