package com.blazingdb.calcite.application;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class GetRelationalAlgebraTest {

  private RelationalAlgebraGenerator relationalAlgebraGenerator;

  private final String queryString;
  private final String expectedMessage;

  protected GetRelationalAlgebraTest(final String queryString,
                                     final String expectedMessage) {
    this.queryString     = queryString;
    this.expectedMessage = expectedMessage;
  }

  protected void SetUp() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema.add(
                "people", new ReflectiveSchema(new PeopleSchema())))
            .build();

    relationalAlgebraGenerator =
        new RelationalAlgebraGenerator(config, new HepProgramBuilder().build());
  }

  protected void TearDown() { this.relationalAlgebraGenerator = null; }

  protected void throwSqlSyntaxException()
      throws SqlSyntaxException, ValidationException, RelConversionException {
    relationalAlgebraGenerator.getRelationalAlgebra(this.queryString);
  }

  protected void hasStartErrorPositionInMessage()
      throws SqlSyntaxException, ValidationException, RelConversionException {
    try {
      relationalAlgebraGenerator.getRelationalAlgebra(this.queryString);
      fail();
    } catch (SqlSyntaxException e) {
      assertThat(e.toString(), containsString(this.expectedMessage));
    }
  }
}
