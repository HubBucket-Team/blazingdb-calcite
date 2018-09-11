package com.blazingdb.calcite.catalog.domain;

import java.util.Set;

public interface Database {

	public String getDatabaseName();

	public Set<Table> getTables();

}
