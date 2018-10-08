package com.blazingdb.calcite.catalog.domain;

public enum CatalogColumnDataType {

	//TODO: handle situations where our column type is timestamp of not the default millisecond resolution
		GDF_INT8,
	    GDF_INT16,
	    GDF_INT32,
	    GDF_INT64,
	    GDF_FLOAT32,
	    GDF_FLOAT64,
	    GDF_DATE32,   	/**< int32_t days since the UNIX epoch */
	    GDF_DATE64,   	/**< int64_t milliseconds since the UNIX epoch */
	    GDF_TIMESTAMP,	/**< Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond) */
	    GDF_CATEGORY,
	    GDF_STRING

}
