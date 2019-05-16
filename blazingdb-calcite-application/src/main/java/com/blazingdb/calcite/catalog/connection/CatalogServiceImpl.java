package com.blazingdb.calcite.catalog.connection;

import java.util.Collection;

import com.blazingdb.calcite.application.RelationalAlgebraGenerator;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTable;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.calcite.schema.BlazingSchema;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;

public class CatalogServiceImpl {

	private DatabaseRepository repo;
	private CatalogDatabaseImpl db = null;
	
	/**
	 * Stores the schema with all the tables and their definitions.
	 */
	private BlazingSchema schema;
	/**
	 * Used to take sql and convert it to optimied relational algebra logical plans.
	 */
	private RelationalAlgebraGenerator relationalAlgebraGenerator;
	
	public CatalogServiceImpl(final String dataDirectory) {
		repo = new DatabaseRepository(dataDirectory);
		db = repo.getDatabase("main");	
		if(db == null) {
			db = new CatalogDatabaseImpl("main");
			this.createDatabase(db);
		}
		
		schema = new BlazingSchema(db);
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);

	}
	
	public RelationalAlgebraGenerator getAlgebraGenerator() {
		return this.relationalAlgebraGenerator;
	}
	
	public synchronized void createTable(DDLCreateTableRequestMessage message) throws Exception{
		
		
		CatalogTableImpl table =  new CatalogTableImpl(message.getName(), db, message.getColumnNames(),message.getColumnTypes());
		db.addTable(table);
		updateSchema();
		//repo.updateDatabase(db);
	}
	
	/**
	 * Refreshes the schema after ddl has occurred. The application context needs to reload
	 * every time we update the ddl right now. Synchronizes access to avoid two threads doing this at once.
	 * 
	 */
	private synchronized void updateSchema() {
		schema = new BlazingSchema(db);
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);

	}
	
	public synchronized void dropTable(DDLDropTableRequestMessage message) throws Exception{
		db.removeTable(message.getName());
		//repo.updateDatabase(db);
		updateSchema();
	}
	
	public CatalogSchemaImpl getSchema(String schemaName) {
		return null;
	}

	// for calcite schema get subschemas
	public CatalogDatabaseImpl getDatabase( String databaseName) {
		return db;
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
