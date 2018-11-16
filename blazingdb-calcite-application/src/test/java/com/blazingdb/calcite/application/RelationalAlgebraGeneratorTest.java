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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class RelationalAlgebraGeneratorTest {

  private RelationalAlgebraGenerator relationalAlgebraGenerator;

  private final String queryString;
  private final String expectedMessage;

  public RelationalAlgebraGeneratorTest(final String queryString,
                                        final String expectedMessage) {
    this.queryString = queryString;
    this.expectedMessage = expectedMessage;
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

    relationalAlgebraGenerator =
        new RelationalAlgebraGenerator(config, new HepProgramBuilder().build());
  }

  @After
  public void TearDown() {
    this.relationalAlgebraGenerator = null;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"select * from heroes whera age=1",
         "Encountered \"age\" at line 1, column 28."},
        {"select *\n  fram heroes\n  whera age=1\n  limit 1",
         "Encountered \"heroes\" at line 2, column 8."},
    });
  }

  @Test(expected = SqlSyntaxException.class)
  public void throwSqlSyntaxException()
      throws SqlSyntaxException, ValidationException, RelConversionException {
    relationalAlgebraGenerator.getRelationalAlgebra(this.queryString);
  }

  @Test
  public void hasStartErrorPositionInMessage()
      throws SqlSyntaxException, ValidationException, RelConversionException {
    try {
      relationalAlgebraGenerator.getRelationalAlgebra(this.queryString);
      fail();
    } catch (SqlSyntaxException e) {
      assertThat(e.toString(), containsString(this.expectedMessage));
    }
  }
}
