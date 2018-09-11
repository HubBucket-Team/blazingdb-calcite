package com.blazingdb.calcite.catalog.domain;

import java.util.Set;

public interface Schema {

	public String getTableName();

	public Set<Table> getDatabases();

}
