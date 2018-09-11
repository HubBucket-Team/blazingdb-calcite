package com.blazingdb.calcite.catalog.connection;

import java.util.Collection;

import com.blazingdb.calcite.catalog.domain.Database;
import com.blazingdb.calcite.catalog.domain.Schema;
import com.blazingdb.calcite.catalog.domain.Table;

public interface CatalogService {

	public Schema getSchema(String schemaName);

	// for calcite schema get subschemas
	public Database getDatabase(String schemaName, String databaseName);

	public Collection<Table> getTables(String databaseName);

	public Table getTable(String schemaName, String tableName);

	// TODO we may not need this api
	public Table getTable(String tableName);

}
