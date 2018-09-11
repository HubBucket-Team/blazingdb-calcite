package com.blazingdb.calcite.catalog.domain;

import java.util.Set;

public interface Table {

	public String getTableName();

	public Set<Column> getColumns();

}
