package com.blazingdb.calcite.application;

import static org.junit.Assert.fail;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.tools.RelConversionException;

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
  public void invalidSyntax() {
    try {
      relationalAlgebraGenerator.getRelationalAlgebra("select * from heroes");
    } catch (SqlParseException e) {
      fail("parsing");
    } catch (ValidationException e) {
      fail("validating");
    } catch (RelConversionException e) {
      fail("internal sql to relational conversion");
    }
  }
}
