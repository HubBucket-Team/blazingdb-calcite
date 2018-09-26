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
@Table(name = "blazing_catalog_schemas")
public class CatalogSchemaImpl implements CatalogSchema {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@ManyToMany(mappedBy = "blazing_catalog_databases", targetEntity = CatalogDatabaseImpl.class)
	private Set<CatalogDatabase> databases;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String getSchemaName() {
		return this.name;
	}

	public void getSchemaName(String name) {
		this.name = name;
	}

	@Override
	public Set<CatalogDatabase> getDatabases() {
		return this.databases;
	}

	public void setDatabases(Set<CatalogDatabase> databases) {
		this.databases = databases;
	}

}
