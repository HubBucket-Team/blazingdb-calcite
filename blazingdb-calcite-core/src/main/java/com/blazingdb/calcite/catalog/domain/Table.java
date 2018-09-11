package com.blazingdb.calcite.catalog.domain;

import java.util.Collection;

public interface Table {

	public String tableName();

	public Collection<Column> getColumns();

}
