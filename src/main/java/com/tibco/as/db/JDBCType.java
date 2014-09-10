package com.tibco.as.db;

import java.sql.Types;

public enum JDBCType {

	ARRAY(Types.ARRAY, "ARRAY"), BIGINT(Types.BIGINT, "BIGINT"), BINARY(
			Types.BINARY, "BINARY"), BIT(Types.BIT, "BIT"), BLOB(Types.BLOB,
			"BLOB"), BOOLEAN(Types.BOOLEAN, "BOOLEAN"), CHAR(Types.CHAR, "CHAR"), CLOB(
			Types.CLOB, "CLOB"), DATALINK(Types.DATALINK, "DATALINK"), DATE(
			Types.DATE, "DATE"), DECIMAL(Types.DECIMAL, "DECIMAL"), DISTINCT(
			Types.DISTINCT, "DISTINCT"), DOUBLE(Types.DOUBLE,
			"DOUBLE PRECISION"), FLOAT(Types.FLOAT, "FLOAT"), INTEGER(
			Types.INTEGER, "INTEGER"), LONGNVARCHAR(Types.LONGNVARCHAR,
			"LONGNVARCHAR"), LONGVARBINARY(Types.LONGVARBINARY, "LONGVARBINARY"), LONGVARCHAR(
			Types.LONGVARCHAR, "LONGVARCHAR"), NCHAR(Types.NCHAR, "NCHAR"), NCLOB(
			Types.NCLOB, "NCLOB"), NULL(Types.NULL, "NULL"), NUMERIC(
			Types.NUMERIC, "NUMERIC"), NVARCHAR(Types.NVARCHAR, "NVARCHAR"), OTHER(
			Types.OTHER, "OTHER"), REAL(Types.REAL, "REAL"), ROWID(Types.ROWID,
			"ROWID"), SMALLINT(Types.SMALLINT, "SMALLINT"), SQLXML(
			Types.SQLXML, "SQLXML"), STRUCT(Types.STRUCT, "STRUCT"), TIME(
			Types.TIME, "TIME"), TIMESTAMP(Types.TIMESTAMP, "TIMESTAMP"), TINYINT(
			Types.TINYINT, "TINYINT"), VARBINARY(Types.VARBINARY, "VARBINARY"), VARCHAR(
			Types.VARCHAR, "VARCHAR");

	private int type;

	private String name;

	private JDBCType(int type, String name) {
		this.type = type;
		this.name = name;
	}

	public int getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public static JDBCType valueOf(int type) {
		for (JDBCType jdbcType : JDBCType.values()) {
			if (jdbcType.getType() == type) {
				return jdbcType;
			}
		}
		return null;
	}
}
