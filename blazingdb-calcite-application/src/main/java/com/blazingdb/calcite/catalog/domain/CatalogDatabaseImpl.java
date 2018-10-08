package com.blazingdb.calcite.catalog.domain;

import java.util.Set;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Table;

import org.hibernate.annotations.Cascade;


import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapKey;
import javax.persistence.OneToMany;

@Entity
@Table(name = "blazing_catalog_database")
public class CatalogDatabaseImpl implements CatalogDatabase {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@OneToMany(fetch = FetchType.EAGER, mappedBy = "database", orphanRemoval = true)
	@Cascade({ org.hibernate.annotations.CascadeType.ALL})
	@MapKey(name="name")
	private Map<String,CatalogTableImpl> databaseTables;

	/*
	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "schema_id")
	private CatalogSchemaImpl schema;
	

	public CatalogSchemaImpl getSchema() {
		return schema;
	}

	public void setSchema(CatalogSchemaImpl schema) {
		this.schema = schema;
	}
	
	*/

	public CatalogDatabaseImpl() {
		this.databaseTables = new HashMap<String,CatalogTableImpl>();		
	}
	public CatalogDatabaseImpl(String name) {
		this.name = name;
		this.databaseTables = new HashMap<String,CatalogTableImpl>();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String getDatabaseName() {
		return this.name;
	}

	public void setDatabaseName(String name) {
		this.name = name;
	}

	public Set<CatalogTable> getTables() {
		Set<CatalogTable> tempTables = new LinkedHashSet<CatalogTable>();
		tempTables.addAll(this.databaseTables.values());
		return tempTables;
	}

	public Map<String,CatalogTableImpl> getDatabaseTables(){
		return this.databaseTables;
	}
	
	public void setDatabaseTables(Map<String,CatalogTableImpl> tables) {
		this.databaseTables = tables;
	}

	// TODO percy move these to a services class
	//TODO felipe thinks its ok to have this here
	@Override
	public CatalogTableImpl getTable(String tableName) {
		// TODO Auto-generated method stub
		return this.databaseTables.get(tableName);
	}

	@Override
	public Set<String> getTableNames() {
		// TODO Auto-generated method stub
		Set<String> tableNames = new LinkedHashSet<String>();
		tableNames.addAll(this.databaseTables.keySet());
		return tableNames;
	}

}
