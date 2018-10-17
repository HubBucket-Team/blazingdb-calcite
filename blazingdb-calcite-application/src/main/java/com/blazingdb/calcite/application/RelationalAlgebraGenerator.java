package com.blazingdb.calcite.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinExtractFilterRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
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
import org.apache.calcite.tools.RelBuilder;
import com.blazingdb.calcite.schema.BlazingSchema;


public class RelationalAlgebraGenerator {


	private Planner planner;
	private HepProgram program;
	private FrameworkConfig config;
	
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
	    	    sqlOperatorTables
	    	      .add(new CalciteCatalogReader(CalciteSchema.from(schema), defaultSchema, new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT), new CalciteConnectionConfigImpl(props)));
	    	    
	    	    
	    	config =Frameworks.newConfigBuilder().defaultSchema(schema)
	      	      .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
	      	      .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables)).build();
	    	
	    	

	  
	 
	        planner = Frameworks.getPlanner(config);
	        

//these were the rules i found in the foo code from BlazingSQLParser			
	        program = new HepProgramBuilder()
	        		.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
	        		.addRuleInstance(FilterJoinRule.JOIN)
	        		.addRuleInstance(JoinPushExpressionsRule.INSTANCE)
	        		.addRuleInstance(JoinExtractFilterRule.INSTANCE)
	        		.addRuleInstance(ProjectMergeRule.INSTANCE)
	        		.addRuleInstance(FilterProjectTransposeRule.INSTANCE)
	        		//.addRuleInstance(SubQueryRemoveRule.)
	                .build();
	        
	        

		}catch(Exception e) {
			e.printStackTrace();
		
			config = null;
			planner = null;
			program = null;
		}
	}
	
	public RelNode getRelationalAlgebra(String sql) throws Exception {
			
	    	
	    	SqlNode tempNode = planner.parse(sql);
	    	
	    	SqlNode validatedSqlNode = planner.validate(tempNode);
	  
	  
	  
	    	RelNode node = planner.rel(validatedSqlNode).project();
	    	System.out.println("non optimized");
	    	System.out.println(RelOptUtil.toString(node));
	    	
	    	final HepPlanner hepPlanner =
	    	        new HepPlanner(program,  config.getContext());
	    	    hepPlanner.setRoot(node);
	    	    node = hepPlanner.findBestExp();  	
		    	System.out.println("optimized");
	    	    System.out.println(RelOptUtil.toString(node));
		    	
	    	   
	    	    /*builder
	    	  .scan("hr", "emps")
	    	  .scan("hr", "joiner")
	    	  .join(JoinRelType.LEFT, builder.call(SqlStdOperatorTable.EQUALS,
	    	          builder.field(2,0,"x"),
	    	          builder.field(2,1,"join_x")))
	    	  .scan("hr", "joiner")
	    	  .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.EQUALS,
	    	          builder.field(2,0,"join_x"),
	    	          builder.field(2,1,"join_x")))
	    	  .project(builder.field("x"),builder.field("y"),builder.field(1,0,"join_x"))
	    	  .filter( builder.call(SqlStdOperatorTable.NOT_EQUALS,
	    	          builder.call(SqlStdOperatorTable.MULTIPLY, builder.field("x"), builder.field("x")) ,
	    	          builder.literal(1)),
	    			  builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
	    	    	          builder.field("y"),
	    	    	          builder.literal(2)))
	    	  
	    	  .build(); */

	    	    return node;
		
	}
}
