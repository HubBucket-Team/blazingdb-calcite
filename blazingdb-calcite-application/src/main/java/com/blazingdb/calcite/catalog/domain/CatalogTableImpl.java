package com.blazingdb.calcite.catalog.domain;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapKey;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Cascade;

@Entity
@Table(name = "blazing_catalog_tables")
public class CatalogTableImpl implements CatalogTable {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@OneToMany(fetch = FetchType.EAGER, mappedBy = "table")
	@Cascade({ org.hibernate.annotations.CascadeType.SAVE_UPDATE })
	@MapKey(name="name") //here this is the column name inside of CatalogColumn
	private Map<String,CatalogColumnImpl> tableColumns;

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "database_id")
	private CatalogDatabaseImpl database;
	
	@Override
	public CatalogDatabaseImpl getDatabase() {
		return database;
	}

	public void setDatabase(CatalogDatabaseImpl database) {
		this.database = database;
	}

	public Long getId() {
		return this.id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String getTableName() {
		return this.name;
	}

	public void setTableName(String name) {
		this.name = name;
	}

	@Override
	public Set<CatalogColumn> getColumns() {
		Set<CatalogColumn> tempColumns = new HashSet<CatalogColumn>();
		tempColumns.addAll(this.tableColumns.values());
		return tempColumns;
	}

	public Map<String,CatalogColumnImpl> getTableColumns(){ //i think hibernate needs a getter of the private data type not sure about this
		return tableColumns;
	}
	
	public void setTableColumns(Map<String,CatalogColumnImpl> columns) {
		this.tableColumns = columns;
	}

}
