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

public class SerializerTest {

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
    final String queryString = "(select name from heroes where age = 1)"
                               + " union"
                               + "(select name from heroes group by name)";

    SqlNode node = planner.parse(queryString);
    node         = planner.validate(node);

    RelNode relNode = planner.rel(node).project();

    PlanRelShuttle visitor = new PlanRelShuttle();
    relNode.accept(visitor);
    StringBuilder builder = new StringBuilder();
    appendTo(builder, visitor.getRootNode());

    assertEquals(builder.toString(),
                 "RootNode\n"
                     + "  UnionNode : all = false\n"
                     + "    ProjectNode : $0\n"
                     + "      FilterNode : =(CAST($1):INTEGER NOT NULL, 1)\n"
                     + "        TableScanNode : path = people.HEROES\n"
                     + "    AggregateNode : groups 0\n"
                     + "      ProjectNode : $0\n"
                     + "        TableScanNode : path = people.HEROES");
  }

  private void appendTo(final StringBuilder builder, final Node node) {
    appendTo(builder, node, "");
  }

  private void appendTo(final StringBuilder builder,
                        final Node node,
                        final String indentation) {
    builder.append(indentation);
    builder.append(node);
    for (final Node child : node.getChildren()) {
      builder.append('\n');
      appendTo(builder, child, indentation + "  ");
    }
  }
}
