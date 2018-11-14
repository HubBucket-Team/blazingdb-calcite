package com.blazingdb.calcite.application;

import static org.junit.Assert.*;

import com.blazingdb.calcite.catalog.connection.CatalogServiceImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;

import com.blazingdb.calcite.schema.BlazingSchema;

import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RelationalAlgebraGeneratorTest {

  private CatalogServiceImpl catalogService;
  private CatalogDatabaseImpl catalogDatabase;

  private BlazingSchema blazingSchema;

  private RelationalAlgebraGenerator relationalAlgebraGenerator;

  @Before
  public void SetUp() {
    catalogDatabase = new CatalogDatabaseImpl("main");

    catalogService = new CatalogServiceImpl();
    catalogService.createDatabase(catalogDatabase);

    ArrayList<String> columnNames = new ArrayList<String>();
		columnNames.add("name");
		columnNames.add("age");
    ArrayList<String> typeNames = new ArrayList<String>();
		typeNames.add("STRING");
		typeNames.add("UNSIGNED SHORT INT");
    DDLCreateTableRequestMessage ddlCreateTableRequestMessage =
        new DDLCreateTableRequestMessage(columnNames, typeNames, "people",
                                         "main");

		try {
			catalogService.createTable(ddlCreateTableRequestMessage);
		} catch(Exception e) {
			System.out.println("Error creating table");
		}

    blazingSchema = new BlazingSchema(catalogDatabase);

    relationalAlgebraGenerator = new RelationalAlgebraGenerator(blazingSchema);
  }

	@After
	public void TearDown() {
		DDLDropTableRequestMessage ddlDropTableRequestMessage = new DDLDropTableRequestMessage("people", "main");
		try {
			catalogService.dropTable(ddlDropTableRequestMessage);
		} catch(Exception e) {
			System.out.println("Error dropping table");
		}
	}

  @Test
  public void invalidSyntax() throws Exception {
    relationalAlgebraGenerator.getRelationalAlgebra("select * from people limit 1");
  }
}
