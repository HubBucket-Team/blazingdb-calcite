package com.blazingdb.calcite.catalog.connection;

import java.util.Collection;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTable;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;

public class CatalogServiceImpl {

	DatabaseRepository repo;
	
	public CatalogServiceImpl(final String dataDirectory) {
		repo = new DatabaseRepository(dataDirectory);
	}
	
	public void createTable(DDLCreateTableRequestMessage message) throws Exception{
		CatalogDatabaseImpl db = repo.getDatabase(message.getDbName());
		
		CatalogTableImpl table =  new CatalogTableImpl(message.getName(), db, message.getColumnNames(),message.getColumnTypes());
		db.addTable(table);
		repo.updateDatabase(db);
	}
	
	public void dropTable(DDLDropTableRequestMessage message) throws Exception{
		CatalogDatabaseImpl db = repo.getDatabase(message.getDbName());
		db.removeTable(message.getName());
		repo.updateDatabase(db);

	}
	
	public CatalogSchemaImpl getSchema(String schemaName) {
		return null;
	}

	// for calcite schema get subschemas
	public CatalogDatabaseImpl getDatabase( String databaseName) {
		return repo.getDatabase(databaseName);
	}

	public Collection<CatalogTable> getTables(String databaseName) {
		return null;
	}

	public CatalogTableImpl getTable(String schemaName, String tableName) {
		return null;
	}

	// TODO we may not need this api
	public CatalogTableImpl getTable(String tableName) {
		return null;
	}

	public void createDatabase(CatalogDatabaseImpl db) {
		repo.createDatabase(db);
		
	}

}
