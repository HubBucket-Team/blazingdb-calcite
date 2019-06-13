package com.blazingdb.calcite.application;

import com.blazingdb.calcite.schema.BlazingSchema;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <h1>Generate Relational Algebra</h1>
 * The purpose of this class is to hold the planner, the program, and the
 * configuration for reuse based on a schema that is provided. It can then take
 * sql and convert it to relational algebra.
 *
 *
 * @author  Felipe Aramburu
 * @version 1.0
 * @since   2018-10-31
 */
public class RelationalAlgebraGenerator {

  /**
   * Planner that converts sql to relational algebra.
   */
  private Planner planner;
  /**
   * Program which takes relational algebra and optimizes it
   */
  private HepProgram program;
  /**
   * Stores the context for the program hep planner. E.G. it stores the schema.
   */
  private FrameworkConfig config;

  /**
   * Constructor for the relational algebra generator class. It will take the
   * schema store it in the  {@link #config} and then set up the  {@link
   * #program} for optimizing and the  {@link #planner} for parsing.
   *
   * @param newSchema This is the schema which we will be using to validate our
   *     query against.
   * 					This gets stored in the {@link #config}
   */
  public RelationalAlgebraGenerator(BlazingSchema newSchema) {
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");

      Properties info = new Properties();
      info.setProperty("lex", "JAVA");
      Connection connection =
          DriverManager.getConnection("jdbc:calcite:", info);
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConnection.getRootSchema();


      schema.add(newSchema.getName(), newSchema);

      // schema.add("EMP", table);
      List<String> defaultSchema = new ArrayList<String>();
      defaultSchema.add(newSchema.getName());

      Properties props = new Properties();
      props.setProperty("defaultSchema", newSchema.getName());
      List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
      sqlOperatorTables.add(SqlStdOperatorTable.instance());
      sqlOperatorTables.add(new CalciteCatalogReader(
          CalciteSchema.from(schema),
          defaultSchema,
          new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
          new CalciteConnectionConfigImpl(props)));


      config =
          Frameworks.newConfigBuilder()
              .defaultSchema(schema)
              .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
              .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
              .build();


      planner = Frameworks.getPlanner(config);


      // these were the rules i found in the foo code from BlazingSQLParser
      program = new HepProgramBuilder()
                    .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
                    .addRuleInstance(
                        FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN)
                    .addRuleInstance(
	                        FilterJoinRule.JoinConditionPushRule.JOIN)

                   .addRuleInstance(ProjectMergeRule.INSTANCE)
                   .addRuleInstance(FilterMergeRule.INSTANCE)
                   .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
                    //.addRuleInstance(FilterTableScanRule.INSTANCE) //turn this on when we want to enable data skipping
                    .addRuleInstance(ProjectTableScanRule.INSTANCE)
                    

                    //.addRuleInstance(SubQueryRemoveRule.)
                    .build();


    } catch (Exception e) {
      e.printStackTrace();

      config  = null;
      planner = null;
      program = null;
    }
  }

  /** Direct constructor for testing purposes */
  public RelationalAlgebraGenerator(FrameworkConfig frameworkConfig,
                                    HepProgram      hepProgram) {
    this.config  = frameworkConfig;
    this.planner = Frameworks.getPlanner(frameworkConfig);
    this.program = hepProgram;
  }

  /**
   * Takes a sql statement and converts it into an optimized relational algebra
   * node. The result of this function is a logical plan that has been optimized
   * using a rule based optimizer.
   *
   * @param sql a string sql query that is to be parsed, converted into
   *     relational algebra, then optimized
   * @return a RelNode which contains the relational algebra tree generated from
   *     the sql statement provided after
   * 			an optimization step has been completed.
   * @throws SqlSyntaxException, SqlValidationException, RelConversionException
   */
  public RelNode getRelationalAlgebra(String sql)
      throws SqlSyntaxException, SqlValidationException,
             RelConversionException {
    SqlNode tempNode;
    try {
      tempNode = planner.parse(sql);
    } catch (SqlParseException e) {
      throw new SqlSyntaxException(sql, e);
    }

    SqlNode validatedSqlNode;
    try {
      validatedSqlNode = planner.validate(tempNode);
    } catch (ValidationException e) {
      throw new SqlValidationException(sql, e);
    }

    RelNode node = planner.rel(validatedSqlNode).project();
    System.out.println("non optimized");
    System.out.println(RelOptUtil.toString(node));

    final HepPlanner hepPlanner = new HepPlanner(program, config.getContext());
    hepPlanner.setRoot(node);
    node = hepPlanner.findBestExp();
    System.out.println("optimized");
    System.out.println(RelOptUtil.toString(node));

    planner.close();

    return node;
  }
}
