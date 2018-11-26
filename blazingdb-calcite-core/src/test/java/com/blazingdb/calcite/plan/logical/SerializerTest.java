package com.blazingdb.calcite.plan.logical;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.adapter.java.ReflectiveSchema;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public final class SerializerTest {

  private Planner planner;

  private final String queryString;
  private final String expectedResult;

  public SerializerTest(final String queryString, final String expectedResult) {
    this.queryString    = queryString;
    this.expectedResult = expectedResult;
  }

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

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"(select age, name from heroes where age = 1)"
             + " union"
             + "(select age, name from heroes group by name, age)",
         "Root\n"
             + "  Union : all = false\n"
             + "    Project : AGE=1, NAME=0\n"
             + "      Filter : Root\n"
             + "        |  Equals\n"
             + "        |    Cast: TARGET=Integer\n"
             + "        |      Reference: INDEX=$1\n"
             + "        |    Literal: DIGEST=1\n"
             + "        |\n"
             + "        TableScan : path = people.HEROES\n"
             + "    Project : AGE=1, NAME=0\n"
             + "      Aggregate : groups = 0, 1\n"
             + "        TableScan : path = people.HEROES\n"},
        {"select * from heroes where age = 2 or age = 0",
         "Root\n"
             + "  Project : NAME=0, AGE=1\n"
             + "    Filter : Root\n"
             + "      |  OrExpression\n"
             + "      |    Equals\n"
             + "      |      Cast: TARGET=Integer\n"
             + "      |        Reference: INDEX=$1\n"
             + "      |      Literal: DIGEST=2\n"
             + "      |    Equals\n"
             + "      |      Cast: TARGET=Integer\n"
             + "      |        Reference: INDEX=$1\n"
             + "      |      Literal: DIGEST=0\n"
             + "      |\n"
             + "      TableScan : path = people.HEROES\n"},
    });
  }

  @Test
  public void test() throws Exception {
    SqlNode node = planner.parse(queryString);
    node         = planner.validate(node);

    RelNode relNode = planner.rel(node).project();

    PlanRelShuttle planRelShuttle = new PlanRelShuttle();
    relNode.accept(planRelShuttle);

    NodeStringSerializer nodeStringSerializer =
        new NodeStringSerializer(planRelShuttle.getRootNode());
    assertEquals(expectedResult, nodeStringSerializer.toString());
  }
}
