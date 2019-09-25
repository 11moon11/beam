package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.common.collect.ImmutableList;
import com.sun.istack.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BigQueryIOSourceRel;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.*;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
            BeamCalcRel.class,
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

    // Does not have good enough cost to be chosen.
    call.getPlanner().setImportance(call.getPlanner().getRoot(), 0);
    call.getPlanner().setRoot(result);
    call.getPlanner().setImportance(result, 1);
    //call.transformTo(result);
  }

  /**
   * Generate new RowType
   * @param calc Calc
   * @param selectedFields List used by BigQuery for project
   * @return RowType to replace current BigQuery output and Calc input
   */
  private RelDataType newRowTypeForBigQuery(Calc calc, List<String> selectedFields) {
    RexProgram program = calc.getProgram();
    RelDataTypeFactory.Builder relDataTypeBuilder = calc.getCluster().getTypeFactory().builder();

    // All projects still need to be present in an output
    List<Pair<RexLocalRef, String>> namedProjectList = program.getNamedProjects();
    for (Pair<RexLocalRef, String> namedProject : namedProjectList) {
      // TODO: Right now: assumes all projects are simple (ex: RexInputRef). Need to handle projection of complex Rex (ex: RexCall)
      relDataTypeBuilder.add(namedProject.right, namedProject.left.getType());
      selectedFields.add(namedProject.right);
    }

    // TODO: Modify to do filter push-down
    // Search through operands of composite nodes to find field names used for conditions
    if (program.getCondition() != null) {
      Queue<RexNode> prerequisites = new ArrayDeque<>();
      // TODO: Can be rewritten using `final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();` for simplicity
      prerequisites.add(program.getExprList().get(program.getCondition().getIndex()));

      while (!prerequisites.isEmpty()) {
        RexNode condNode = prerequisites.poll();
        if (condNode instanceof RexCall) {
          RexCall composites = (RexCall) condNode;
          // Could use something like this to map to new operands:
          //RexCall newComposite = composites.clone(composites.getType(), composites.getOperands());
          for (RexNode dependant : composites.getOperands()) {
            if (dependant instanceof RexLocalRef) {
              prerequisites.add(program.getExprList().get(((RexLocalRef) dependant).getIndex()));
            }
          }
        } else if (condNode instanceof RexInputRef) {
          String fieldName = calc.getInput().getRowType().getFieldList()
              .get(((RexInputRef) condNode).getIndex()).getName();
          relDataTypeBuilder.add(fieldName, condNode.getType());
          selectedFields.add(fieldName);
        }
      }
    }

    return relDataTypeBuilder.build();
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
    bigQueryTable.setMethod(Method.DIRECT_READ);
    bigQueryTable.setSelectedFields(selectedFields);

    BigQueryIOSourceRel bigQueryIOSourceRel = new BigQueryIOSourceRel(ioSourceRel.getCluster(),
        ioSourceRel.getTable(),
        bigQueryTable,
        ioSourceRel.getPipelineOptions(),
        ioSourceRel.getCalciteTable());

    bigQueryIOSourceRel.setRowType(calcInput);

    return bigQueryIOSourceRel;
  }

  private Calc constructNewCalc(Calc currentCalc, BigQueryIOSourceRel bigQueryIOSourceRel) {
    RexProgram program = currentCalc.getProgram();

    List<RexNode> list = program.getExprList();
    RexBuilder rexBuilder = currentCalc.getCluster().getRexBuilder();
    RexProgramBuilder rexProgramBuilder = new RexProgramBuilder(bigQueryIOSourceRel.getRowType(), rexBuilder);
    SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
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
