package com.tibco.as.db;

import java.sql.Types;

public enum JDBCType {

	ARRAY(Types.ARRAY), BIGINT(Types.BIGINT), BINARY(Types.BINARY), BIT(
			Types.BIT), BLOB(Types.BLOB), BOOLEAN(Types.BOOLEAN), CHAR(
			Types.CHAR), CLOB(Types.CLOB), DATALINK(Types.DATALINK), DATE(
			Types.DATE), DECIMAL(Types.DECIMAL), DISTINCT(Types.DISTINCT), DOUBLE(
			Types.DOUBLE), FLOAT(Types.FLOAT), INTEGER(Types.INTEGER), JAVA_OBJECT(
			Types.JAVA_OBJECT), LONGNVARCHAR(Types.LONGNVARCHAR), LONGVARBINARY(
			Types.LONGVARBINARY), LONGVARCHAR(Types.LONGVARCHAR), NCHAR(
			Types.NCHAR), NCLOB(Types.NCLOB), NULL(Types.NULL), NUMERIC(
			Types.NUMERIC), NVARCHAR(Types.NVARCHAR), OTHER(Types.OTHER), REAL(
			Types.REAL), REF(Types.REF), ROWID(Types.ROWID), SMALLINT(
			Types.SMALLINT), SQLXML(Types.SQLXML), STRUCT(Types.STRUCT), TIME(
			Types.TIME), TIMESTAMP(Types.TIMESTAMP), TINYINT(Types.TINYINT), VARBINARY(
			Types.VARBINARY), VARCHAR(Types.VARCHAR);

	private int type;

	private JDBCType(int type) {
		this.type = type;
	}

	public int getType() {
		return type;
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
