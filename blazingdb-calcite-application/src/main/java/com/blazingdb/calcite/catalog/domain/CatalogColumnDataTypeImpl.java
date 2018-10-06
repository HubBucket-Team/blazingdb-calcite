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
/*
 * NOTE: the data in here can just be generated via liquibase no repository necessary
 */
@Entity
@Table(name = "blazing_catalog_column_datatypes")
public class CatalogColumnDataTypeImpl implements CatalogColumnDataType {


	@Id
	@Column(name = "name", nullable = false)
	private String name;

	public CatalogColumnDataTypeImpl() {
	}

	public CatalogColumnDataTypeImpl(String name) {
		this.name = name;
	}

	@Override
	public String getDataTypeName() {
		return this.name;
	}


	public void getName(String name) {
		this.name = name;
	}
	public void setName(String name) {
		this.name = name;
	}

}
