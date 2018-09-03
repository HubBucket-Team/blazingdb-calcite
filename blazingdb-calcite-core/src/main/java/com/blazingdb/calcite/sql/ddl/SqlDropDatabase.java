package com.blazingdb.calcite.sql.ddl;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code DROP DATABASE} statement.
 */
public class SqlDropDatabase extends SqlDrop implements SqlExecutableStatement {
	private final SqlIdentifier name;

	private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP DATABASE", SqlKind.OTHER);

	/** Creates a SqlDropSchema. */
	SqlDropDatabase(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
		super(OPERATOR, pos, ifExists);
		this.name = name;
	}

	public List<SqlNode> getOperandList() {
		return ImmutableList.<SqlNode>of(name);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DROP");

		writer.keyword("DATABASE");

		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		name.unparse(writer, leftPrec, rightPrec);
	}

	public void execute(CalcitePrepare.Context context) {
		// TODO percy calcite catalog
		// final List<String> path = context.getDefaultSchemaPath();
		// CalciteSchema schema = context.getRootSchema();
		// for (String p : path) {
		// schema = schema.getSubSchema(p, true);
		// }
		// final boolean existed = schema.removeSubSchema(name.getSimple());
		// if (!existed && !ifExists) {
		// throw SqlUtil.newContextException(name.getParserPosition(), RESOURCE.schemaNotFound(name.getSimple()));
		// }
	}
}

// End SqlDropDatabase.java
