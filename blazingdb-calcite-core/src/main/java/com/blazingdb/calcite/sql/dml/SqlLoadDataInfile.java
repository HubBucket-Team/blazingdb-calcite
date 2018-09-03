package com.blazingdb.calcite.sql.dml;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Parse tree for {@code USE DATABASE} statement. Syntax: "load data infile [file] into table [table] fields terminated
 * by [delimiter] enclosed by [quoteCharacter] lines terminated by [termination]"
 * 
 * Where [file], [table], [delimiter], [quoteCharacter], [termination] are values with single quotes ('');
 * 
 * Example: "load data infile 'supplier.tbl' into table supplier fields terminated by '|' enclosed by '"' lines
 * terminated by '\n'"
 */
public class SqlLoadDataInfile extends SqlCall {

	private static final String KEYWORD = "LOAD DATA INFILE";
	private static final SqlOperator OPERATOR = new SqlSpecialOperator(KEYWORD, SqlKind.OTHER);

	private final SqlNode file;
	private final SqlIdentifier table;
	private final SqlNode delimiter;
	private final SqlNode quoteCharacter;
	private final SqlNode termination;

	/** Creates a SqlUseDatabase. */
	SqlLoadDataInfile(SqlParserPos pos, SqlNode file, SqlIdentifier table, SqlNode delimiter, SqlNode quoteCharacter,
			SqlNode termination) {
		super(pos);

		this.file = Preconditions.checkNotNull(file);
		this.table = Preconditions.checkNotNull(table);
		this.delimiter = Preconditions.checkNotNull(delimiter);
		this.quoteCharacter = Preconditions.checkNotNull(quoteCharacter);
		this.termination = Preconditions.checkNotNull(termination);
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.<SqlNode>of(this.file, this.table, this.delimiter, this.quoteCharacter,
				this.termination);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("LOAD");
		writer.keyword("DATA");
		writer.keyword("INFILE");
		this.file.unparse(writer, leftPrec, rightPrec);
		writer.keyword("INTO");
		writer.keyword("TABLE");
		this.table.unparse(writer, leftPrec, rightPrec);
		writer.keyword("FIELDS");
		writer.keyword("TERMINATED");
		writer.keyword("BY");
		this.delimiter.unparse(writer, leftPrec, rightPrec);
		writer.keyword("ENCLOSED");
		writer.keyword("BY");
		this.quoteCharacter.unparse(writer, leftPrec, rightPrec);
		writer.keyword("LINES");
		writer.keyword("TERMINATED");
		writer.keyword("BY");
		this.termination.unparse(writer, leftPrec, rightPrec);
	}
}

// End SqlLoadDataInfile.java
