package com.blazingdb.calcite.catalog.domain;

import java.util.ArrayList;
import java.util.Collection;

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
@Table(name = "blazing_catalog_columns")
public class CatalogColumnImpl implements CatalogColumn {

	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@OneToOne(mappedBy = "blazing_catalog_column_datatypes")
	private CatalogColumnDataTypeImpl dataType;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String getColumnName() {
		return this.name;
	}

	public void setColumnName(String name) {
		this.name = name;
	}

	@Override
	public CatalogColumnDataTypeImpl getColumnDataType() {
		return this.dataType;
	}

	public void setColumnDataType(CatalogColumnDataTypeImpl dataType) {
		this.dataType = dataType;
	}

}
