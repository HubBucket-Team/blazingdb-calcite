package com.blazingdb.calcite.sql.ddl;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE DATABASE} statement.
 */
public class SqlCreateDatabase extends SqlCreate implements SqlExecutableStatement {
	private final SqlIdentifier name;

	private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE DATABASE", SqlKind.OTHER);

	/** Creates a SqlCreateDatabase. */
	SqlCreateDatabase(SqlParserPos pos, SqlIdentifier name) {
		super(OPERATOR, pos, false, false); // TODO percy extend this if we want improve create database
		this.name = Preconditions.checkNotNull(name);
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.<SqlNode>of(name);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		if (getReplace()) {
			writer.keyword("CREATE OR REPLACE");
		} else {
			writer.keyword("CREATE");
		}

		writer.keyword("DATABASE");

		if (ifNotExists) {
			writer.keyword("IF NOT EXISTS");
		}

		name.unparse(writer, leftPrec, rightPrec);
	}

	public void execute(CalcitePrepare.Context context) {
		// TODO percy register this db into the catalog
		// final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
		// final SchemaPlus subSchema0 = pair.left.plus().getSubSchema(pair.right);
		// if (subSchema0 != null) {
		// if (!getReplace() && !ifNotExists) {
		// throw SqlUtil.newContextException(name.getParserPosition(), RESOURCE.schemaExists(pair.right));
		// }
		// }
		// final Schema subSchema = new AbstractSchema();
		// pair.left.add(pair.right, subSchema);
	}
}

// End SqlCreateDatabase.java
