package com.blazingdb.calcite.catalog.connection;

import java.util.Collection;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;

public interface CatalogServiceImpl {

	public CatalogSchemaImpl getSchema(String schemaName);

	// for calcite schema get subschemas
	public CatalogDatabaseImpl getDatabase(String schemaName, String databaseName);

	public Collection<CatalogTableImpl> getTables(String databaseName);

	public CatalogTableImpl getTable(String schemaName, String tableName);

	// TODO we may not need this api
	public CatalogTableImpl getTable(String tableName);

}
