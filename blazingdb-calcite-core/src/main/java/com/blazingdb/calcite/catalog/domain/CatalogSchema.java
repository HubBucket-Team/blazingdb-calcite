package com.blazingdb.calcite.catalog.domain;

import java.util.Set;

public interface CatalogSchema {

	public String getTableName();

	public Set<CatalogTable> getDatabases();

}
