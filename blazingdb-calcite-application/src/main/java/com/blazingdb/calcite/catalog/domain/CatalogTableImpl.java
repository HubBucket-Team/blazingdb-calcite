package com.blazingdb.calcite.catalog.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name = "blazing_catalog_tables")
public class CatalogTableImpl implements CatalogTable {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@ManyToMany(mappedBy = "blazing_catalog_columns", targetEntity = CatalogColumnImpl.class)
	private Set<CatalogColumn> columns;

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
		return this.columns;
	}

	public void setColumns(Set<CatalogColumn> columns) {
		this.columns = columns;
	}

}
