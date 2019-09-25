package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.common.collect.ImmutableList;
import com.sun.istack.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BigQueryIOSourceRel;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.*;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

public class BeamBigQueryProjectRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final BeamBigQueryProjectRule INSTANCE =
      new BeamBigQueryProjectRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  BeamBigQueryProjectRule(RelBuilderFactory relBuilderFactory) {
    super(operand(
            Calc.class,
            operand(BeamIOSourceRel.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Calc calc = call.rel(0);
    final BeamIOSourceRel ioSourceRel = call.rel(1);

    RexProgram program = calc.getProgram();

    BigQueryIOSourceRel bigQueryIOSourceRel = constructNewIO(calc, ioSourceRel);
    // TODO: remove `calc.getProgram().getOutputRowType().getFieldCount() != 1`
    if (bigQueryIOSourceRel == null || calc.getProgram().getOutputRowType().getFieldCount() != 1) {
      return;
    }

    // Can quit here if Calc does nothing besides project (works as intended)
    //call.transformTo(bigQueryIOSourceRel);

    // When calc does something, need to modify inputs of the program
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();
    /*if (projectFilter.right.size() == 0) {
      call.transformTo(bigQueryIOSourceRel);
      return;
    }*/

    Calc result = constructNewCalc(calc, bigQueryIOSourceRel);

    call.transformTo(result);
  }

  /**
   * Generate new RowType for BigQueryIO
   * @param calc Calc
   * @param selectedFields List used by BigQuery for project
   * @return RowType to replace current BigQuery output and Calc input
   */
  private RelDataType newRowTypeForBigQuery(Calc calc, List<String> selectedFields) {
    RexProgram program = calc.getProgram();
    Set<String> requiredFields = new HashSet<>();
    RelDataTypeFactory.Builder relDataTypeBuilder = calc.getCluster().getTypeFactory().builder();

    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();
    // Find all input refs used by projects
    for (RexNode project : projectFilter.left) {
      findUtilizedInputRefs(program, project, requiredFields, relDataTypeBuilder);
    }
    // Find all input refs used by filters
    for (RexNode filter : projectFilter.right) {
      // TODO: Modify to do filter push-down
      findUtilizedInputRefs(program, filter, requiredFields, relDataTypeBuilder);
    }

    selectedFields.addAll(requiredFields);
    return relDataTypeBuilder.build();
  }

  /**
   *
   * @param program Calc program to search through
   * @param startNode Node to start at
   * @param requiredFields Used to keep track of input fields used by the node and it's children
   * @param relDataTypeBuilder Builder for new RelDataType containing only used input fields
   */
  private void findUtilizedInputRefs(RexProgram program, RexNode startNode, Set<String> requiredFields, RelDataTypeFactory.Builder relDataTypeBuilder) {
    Queue<RexNode> prerequisites = new ArrayDeque<>();
    prerequisites.add(startNode);

    // Assuming there are no cyclic nodes, traverse dependency tree until all RexInputRefs are found
    while (!prerequisites.isEmpty()) {
      RexNode node = prerequisites.poll();

      if (node instanceof RexCall) { // Composite expression, example: "=($t11, $t12)"
        RexCall compositeNode = (RexCall) node;

        // Expression from example above contains 2 operands: $t11, $t12
        prerequisites.addAll(compositeNode.getOperands());
      } else if (node instanceof RexInputRef) { // Input reference
        // Find a field in an inputRowType for the input reference
        int inputFieldIndex = ((RexInputRef) node).getIndex();
        RelDataTypeField field = program.getInputRowType().getFieldList().get(inputFieldIndex);

        // If we have not seen it before - add it to the list
        if (!requiredFields.contains(field.getName())) {
          relDataTypeBuilder.add(field.getName(), field.getType());
          requiredFields.add(field.getName());
        }
      }
    }
  }

  /**
   * Construct an optimized BigQueryIO (with Project / Filter operations pushed-down)
   * @param calc Calc following BeamIOSourceRel
   * @param ioSourceRel IO sink, which will be transformed
   * @return BigQueryIOSourceRel on success, null on failure
   */
  @Nullable private BigQueryIOSourceRel constructNewIO(Calc calc, BeamIOSourceRel ioSourceRel) {
    final BeamSqlTable table = ioSourceRel.getBeamSqlTable();
    // If IOSource does not support filter/project push-down - no work to be done
    if(!(table instanceof BigQueryTable)) {
      return null;
    }

    List<String> selectedFields = new ArrayList<>();
    RelDataType calcInput = newRowTypeForBigQuery(calc, selectedFields);
    // If no fields are being used - no work to be done
    if (selectedFields.size() == 0) {
      return null;
    }

    BigQueryTable bigQueryTable = (BigQueryTable) table;

    BigQueryIOSourceRel bigQueryIOSourceRel = new BigQueryIOSourceRel(ioSourceRel.getCluster(),
        ioSourceRel.getTable(),
        bigQueryTable,
        ioSourceRel.getPipelineOptions(),
        ioSourceRel.getCalciteTable(),
        selectedFields);

    bigQueryIOSourceRel.setRowType(calcInput);

    return bigQueryIOSourceRel;
  }

  /**
   * Creates a new Calc with remaining conditions and filters, which were unable to get pushed-down
   * @param currentCalc existing Calc
   * @param bigQueryIOSourceRel new IO with some projects and filters pushed-down
   * @return Calc with the new input and remaining operations
   */
  private Calc constructNewCalc(Calc currentCalc, BigQueryIOSourceRel bigQueryIOSourceRel) {
    RexProgram program = currentCalc.getProgram();

    List<RexNode> list = program.getExprList();
    RexBuilder rexBuilder = currentCalc.getCluster().getRexBuilder();
    RexProgramBuilder rexProgramBuilder = new RexProgramBuilder(bigQueryIOSourceRel.getRowType(), rexBuilder);
    SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // TODO: Construct new program inputs without using hard-coded values
    RexNode rexNode = new RexInputRef(0, sqlTypeFactory.createTypeWithNullability(sqlTypeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    RexNode rexNodeCond = new RexInputRef(1, sqlTypeFactory.createTypeWithNullability(sqlTypeFactory.createSqlType(SqlTypeName.TINYINT), true));

    RexLocalRef project = rexProgramBuilder.registerInput(rexNode);
    rexProgramBuilder.registerInput(rexNodeCond);

    rexProgramBuilder.addProject(0, "$t0"); // TODO: handle SQL 'as'
    RexLocalRef castInputToInt = rexProgramBuilder.addExpr(list.get(11));
    RexLocalRef finalIntToCompareTo = rexProgramBuilder.addExpr(list.get(12));
    RexLocalRef condition = rexProgramBuilder.addExpr(rexBuilder.copy(((RexCall) list.get(13)).clone(list.get(13).getType(), ImmutableList.of(castInputToInt, finalIntToCompareTo))));
    //condition = rexProgramBuilder.registerOutput(condition);

    //rexProgramBuilder.addExpr();
    rexProgramBuilder.addCondition(condition);
    RexProgram builtProgram = rexProgramBuilder.getProgram();

    // TODO: Map `program.getCondition()` to new values

    //RexProgram newProgram = RexProgram.create(bigQueryIOSourceRel.getRowType(), ImmutableList.of(project), builtProgram.getCondition(), builtProgram.getOutputRowType(), currentCalc.getCluster().getRexBuilder());
    // TODO: Remove project from the program, since it is being moved it IO
    final Calc result = currentCalc.copy(currentCalc.getTraitSet(),
        bigQueryIOSourceRel, builtProgram);

    return result;
  }
}
