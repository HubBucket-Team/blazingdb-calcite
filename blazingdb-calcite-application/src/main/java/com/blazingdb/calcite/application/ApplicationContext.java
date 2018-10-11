package com.blazingdb.calcite.application;

import com.blazingdb.calcite.catalog.connection.CatalogServiceImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.schema.BlazingSchema;

public class ApplicationContext {

	private CatalogServiceImpl catalogService;
	//assuming just one database for now
	private BlazingSchema schema;
	RelationalAlgebraGenerator relationalAlgebraGenerator;
	private static ApplicationContext instance = null;
	
	private ApplicationContext() {
		catalogService = new CatalogServiceImpl();
		CatalogDatabaseImpl db = catalogService.getDatabase("main");
		if(db == null) {
			db = new CatalogDatabaseImpl("main");
			catalogService.createDatabase(db);
		}
		db = catalogService.getDatabase("main");
		schema = new BlazingSchema(db);
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);
		
	}
	
	public static void init() {
		if(instance == null) {
			instance = new ApplicationContext();
		}
	}
	public static CatalogServiceImpl getCatalogService() {
		
		init();
		return instance.getService();
	}
	
	public static RelationalAlgebraGenerator getRelationalAlgebraGenerator() {
		init();
		return instance.getAlgebraGenerator();
		
	}
	
	private synchronized CatalogServiceImpl getService() {
		return catalogService;
	}

	private synchronized RelationalAlgebraGenerator getAlgebraGenerator() {
		return this.relationalAlgebraGenerator;
	}

	private synchronized void updateSchema() {
		schema = new BlazingSchema(catalogService.getDatabase("main"));
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);
		
	}

	public static void updateContext() {
		// TODO Auto-generated method stub
		init();
		instance.updateSchema();
	}

}
