package com.blazingdb.catalog.domain;

import java.util.Collection;

public interface Schema {

	public String getSchemaName();

	public Collection<Table> getTable();

}
