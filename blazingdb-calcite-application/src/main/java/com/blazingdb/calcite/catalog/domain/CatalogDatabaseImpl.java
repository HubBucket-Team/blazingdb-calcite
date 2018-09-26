package com.blazingdb.calcite.catalog.domain;

import java.util.Set;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.OneToOne;

@Entity
@Table(name = "blazing_catalog_databases")
public class CatalogDatabaseImpl implements CatalogDatabase {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@ManyToMany(mappedBy = "blazing_catalog_columns", targetEntity = CatalogTableImpl.class)
	private Set<CatalogTable> tables;

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
		return this.tables;
	}

	public void setTables(Set<CatalogTable> tables) {
		this.tables = tables;
	}

	// TODO percy move these to a services class
	@Override
	public CatalogTable getTable(String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getTableNames() {
		// TODO Auto-generated method stub
		return null;
	}

}
