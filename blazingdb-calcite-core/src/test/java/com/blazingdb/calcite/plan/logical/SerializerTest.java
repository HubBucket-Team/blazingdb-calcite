package com.blazingdb.calcite.plan.logical;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class SerializerTest {

  private Planner planner;

  @Before
  public void SetUp() {

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema.add(
                "people", new ReflectiveSchema(new PeopleSchema())))
            .build();

    planner = Frameworks.getPlanner(config);
  }

  @After
  public void TearDown() {
    planner = null;
  }

  @Test
  public void test() throws Exception {
    final String queryString =
        "(select age, name from heroes where age = 1)"
        + " union"
        + "(select age, name from heroes group by name, age)";

    SqlNode node = planner.parse(queryString);
    node         = planner.validate(node);

    RelNode relNode = planner.rel(node).project();

    PlanRelShuttle planRelShuttle = new PlanRelShuttle();
    relNode.accept(planRelShuttle);

    NodeStringSerializer nodeStringSerializer =
        new NodeStringSerializer(planRelShuttle.getRootNode());
    assertEquals(nodeStringSerializer.toString(),
                 "RootNode\n"
                     + "  UnionNode : all = false\n"
                     + "    ProjectNode : AGE=1, NAME=0\n"
                     + "      FilterNode : =(CAST($1):INTEGER NOT NULL, 1)\n"
                     + "        TableScanNode : path = people.HEROES\n"
                     + "    ProjectNode : AGE=1, NAME=0\n"
                     + "      AggregateNode : groups = 0, 1\n"
                     + "        TableScanNode : path = people.HEROES\n");
  }
}
