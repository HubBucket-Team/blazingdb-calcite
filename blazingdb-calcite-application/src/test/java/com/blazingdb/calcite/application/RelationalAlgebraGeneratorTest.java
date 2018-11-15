package com.blazingdb.calcite.application;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RelationalAlgebraGeneratorTest {

  private RelationalAlgebraGenerator relationalAlgebraGenerator;

  @Before
  public void SetUp() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema.add(
                "people", new ReflectiveSchema(new PeopleSchema())))
            .build();

    relationalAlgebraGenerator = new RelationalAlgebraGenerator(config);
  }

  @After
  public void TearDown() {
    this.relationalAlgebraGenerator = null;
  }

  @Test
  public void validSyntax() {
    try {
      relationalAlgebraGenerator.getRelationalAlgebra("select * from heroes");
    } catch (SqlSyntaxException e) {
      fail("parsing");
    } catch (ValidationException e) {
      fail("validating");
    } catch (RelConversionException e) {
      fail("internal sql to relational conversion");
    }
  }

  @Test
  public void invalidSelect() {
    try {
      relationalAlgebraGenerator.getRelationalAlgebra(
          "select * from heroes whera age=1");
    } catch (SqlSyntaxException e) {
      assertThat(e.toString(), containsString("line 1, column 28"));
      return;
    } catch (ValidationException e) {
      fail("validating");
    } catch (RelConversionException e) {
      fail("internal sql to relational conversion");
    }
    fail("Unreachable");
  }
}
