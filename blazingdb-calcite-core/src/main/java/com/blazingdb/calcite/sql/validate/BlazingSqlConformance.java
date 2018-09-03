package com.blazingdb.calcite.sql.validate;

import org.apache.calcite.sql.validate.SqlAbstractConformance;

public class BlazingSqlConformance extends SqlAbstractConformance {

	@Override
	public boolean isPercentRemainderAllowed() {
		return true;
	}

}
