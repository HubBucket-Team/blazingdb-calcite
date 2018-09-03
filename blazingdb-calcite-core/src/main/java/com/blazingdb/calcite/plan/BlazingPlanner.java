package com.blazingdb.calcite.plan;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.Compiler;
import org.apache.calcite.interpreter.NoneToBindableConverterRule;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinExtractFilterRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

import com.blazingdb.calcite.schema.CsvSchema;
import com.blazingdb.calcite.schema.CsvTable.Flavor;
import com.blazingdb.calcite.sql.parser.SqlParserImpl;
import com.blazingdb.calcite.sql.validate.BlazingSqlConformance;
import com.google.common.io.Resources;
import com.kenai.jffi.CallingConvention;

public class BlazingPlanner {

	// public static CalciteState sqlOverDummyTable(String sql)
	// throws RelConversionException, ValidationException, SqlParseException {
	// SchemaPlus rootSchema = Frameworks.createRootSchema(true);
	//
	// JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
	// // StreamableTable streamableTable = new CompilerUtil.TableBuilderInfo(typeFactory)
	// // .field("ID", SqlTypeName.INTEGER,
	// // new ColumnConstraint.PrimaryKey(SqlMonotonicity.MONOTONIC, SqlParserPos.ZERO))
	// // .field("NAME", typeFactory.createType(String.class)).field("ADDR", typeFactory.createType(String.class))
	// // .build();
	// // Table table = streamableTable.stream();
	// // schema.add("FOO", table);
	// // schema.add("BAR", table);
	// // schema.add("MYPLUS", ScalarFunctionImpl.create(MyPlus.class, "eval"));
	//
	// // QueryPlanner queryPlanner = new QueryPlanner(schema);
	// // StreamsRel tree = queryPlanner.getPlan(sql);
	// // System.out.println(StormRelUtils.explain(tree, SqlExplainLevel.ALL_ATTRIBUTES));
	// return new CalciteState(schema, null);
	// }

	private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
			.setParserFactory(SqlParserImpl.FACTORY).setCaseSensitive(true).setUnquotedCasing(Casing.UNCHANGED)
			.setQuotedCasing(Casing.UNCHANGED).setQuoting(Quoting.DOUBLE_QUOTE)
			.setConformance(new BlazingSqlConformance()).build();

	void foo(String sql)
			throws RelConversionException, ValidationException, SqlParseException, IOException, SQLException {

		// todo based
		// File directoryFile = new File("/home/percy/Desktop/hol/calcite/example/csv/src/test/resources/sales/");
		// Flavor flavor = Flavor.SCANNABLE;
		// CsvSchema schema = new CsvSchema(directoryFile, flavor);

		TpchSchema schema = new TpchSchema(0.01, 1, 1, true);

		SchemaPlus catalog = Frameworks.createRootSchema(true);
		catalog.add("tpch", schema);

		final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
		traitDefs.add(ConventionTraitDef.INSTANCE);
		traitDefs.add(RelCollationTraitDef.INSTANCE);

		RuleSet rules = RuleSets.ofList(FilterJoinRule.FILTER_ON_JOIN, FilterJoinRule.JOIN,
				JoinPushExpressionsRule.INSTANCE, JoinExtractFilterRule.INSTANCE, ProjectMergeRule.INSTANCE,
				FilterProjectTransposeRule.INSTANCE);

		FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(catalog.getSubSchema("tpch"))
				.parserConfig(PARSER_CONFIG).context(Contexts.EMPTY_CONTEXT).costFactory(null).traitDefs(traitDefs)
				.typeSystem(RelDataTypeSystem.DEFAULT).ruleSets(rules).build();
		Planner planner = Frameworks.getPlanner(config);

		SqlNode a = planner.parse(sql);
		SqlNode aa = planner.validate(a);
		RelRoot root = planner.rel(aa);
		RelNode node = root.project();

		System.out.println(RelOptUtil.toString(node));

		// Program program = Programs.ofRules(FilterJoinRule.FILTER_ON_JOIN);
		//
		RelTraitSet requiredOutputTraits = planner.getEmptyTraitSet();
		// List<RelOptMaterialization> materializations = new ArrayList<RelOptMaterialization>();
		// List<RelOptLattice> lattices = new ArrayList<RelOptLattice>();
		RelNode rel = root.rel;
		// RelOptPlanner volcano = new VolcanoPlanner();
		// RelNode optimized = program.run(volcano, rel, requiredOutputTraits, materializations, lattices);

		System.out.println("OPTIMZED:");

		int ruleSetIndex = 0; // we have only 1 ruleset in the planner
		RelNode optimized = planner.transform(ruleSetIndex, requiredOutputTraits, rel);
		System.out.println(RelOptUtil.toString(optimized));

		planner.close();

		// //CalciteConnection connection = new SimpleCalciteConnection();
		// String salesSchema = Resources.toString(BlazingPlanner.class.getResource("/sales.json"),
		// Charset.defaultCharset());
		// // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default
		// // schema
		// new ModelHandler(connection, "inline:" + salesSchema);
		//
		// SchemaPlus schema = connection.getRootSchema().getSubSchema(connection.getSchema());

		// FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(schema).context(Contexts.EMPTY_CONTEXT)
		// .costFactory(null).build();
		// Planner planner = Frameworks.getPlanner(config);
		//
		// SqlNode a = planner.parse(sql);
		//
		// System.out.println(a.toString());
		//
		// // SqlNode aa = planner.validate(a);
		//
		// RelRoot aaa = planner.rel(a);
		//
		// planner.close();
		// connection.close();
	}

}
