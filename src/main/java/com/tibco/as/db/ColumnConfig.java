package com.tibco.as.db;

import com.tibco.as.io.FieldConfig;
import com.tibco.as.space.FieldDef.FieldType;

public class ColumnConfig extends FieldConfig {

	private static final int DEFAULT_BLOB_SIZE = 255;
	private static final int DEFAULT_CLOB_SIZE = 255;

	private String columnName;
	private JDBCType columnType;
	private Integer columnSize;
	private Integer decimalDigits;
	private Integer radix;
	private Boolean columnNullable;

	public String getColumnName() {
		if (columnName == null) {
			return getFieldName();
		}
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public JDBCType getColumnType() {
		if (columnType == null) {
			switch (getFieldType()) {
			case BLOB:
				return JDBCType.BLOB;
			case BOOLEAN:
				return JDBCType.NUMERIC;
			case CHAR:
				return JDBCType.CHAR;
			case DATETIME:
				return JDBCType.TIMESTAMP;
			case DOUBLE:
				return JDBCType.DOUBLE;
			case FLOAT:
				return JDBCType.REAL;
			case INTEGER:
				return JDBCType.INTEGER;
			case LONG:
				return JDBCType.NUMERIC;
			case SHORT:
				return JDBCType.SMALLINT;
			case STRING:
				return JDBCType.VARCHAR;
			}
		}
		return columnType;
	}

	public void setColumnType(JDBCType columnType) {
		this.columnType = columnType;
	}

	public Integer getColumnSize() {
		if (columnSize == null) {
			switch (getColumnType()) {
			case BINARY:
			case BLOB:
			case LONGVARBINARY:
			case VARBINARY:
				return DEFAULT_BLOB_SIZE;
			case CHAR:
			case CLOB:
			case LONGNVARCHAR:
			case LONGVARCHAR:
			case NCHAR:
			case NCLOB:
			case NVARCHAR:
			case VARCHAR:
				return DEFAULT_CLOB_SIZE;
			default:
				return null;
			}
		}
		return columnSize;
	}

	public void setColumnSize(Integer columnSize) {
		this.columnSize = columnSize;
	}

	public Integer getDecimalDigits() {
		return decimalDigits;
	}

	public void setDecimalDigits(Integer decimalDigits) {
		this.decimalDigits = decimalDigits;
	}

	public Integer getRadix() {
		return radix;
	}

	public void setRadix(Integer radix) {
		this.radix = radix;
	}

	public Boolean getColumnNullable() {
		if (columnNullable == null) {
			return getFieldNullable();
		}
		return columnNullable;
	}

	public void setColumnNullable(Boolean columnNullable) {
		this.columnNullable = columnNullable;
	}

	@Override
	public ColumnConfig clone() {
		ColumnConfig clone = new ColumnConfig();
		copyTo(clone);
		return clone;
	}

	public void copyTo(ColumnConfig target) {
		target.columnName = columnName;
		target.columnNullable = columnNullable;
		target.columnSize = columnSize;
		target.columnType = columnType;
		target.decimalDigits = decimalDigits;
		target.radix = radix;
		super.copyTo(target);
	}

	@Override
	public String getFieldName() {
		String fieldName = super.getFieldName();
		if (fieldName == null) {
			return columnName;
		}
		return fieldName;
	}

	@Override
	public Boolean getFieldNullable() {
		Boolean fieldNullable = super.getFieldNullable();
		if (fieldNullable == null) {
			return columnNullable;
		}
		return fieldNullable;
	}

	@Override
	public FieldType getFieldType() {
		if (super.getFieldType() == null) {
			switch (columnType) {
			case CHAR:
			case CLOB:
			case LONGVARCHAR:
			case LONGNVARCHAR:
			case NCHAR:
			case NCLOB:
			case NVARCHAR:
			case VARCHAR:
			case SQLXML:
				return FieldType.STRING;
			case NUMERIC:
			case DECIMAL:
				return FieldType.DOUBLE;
			case BIT:
			case BOOLEAN:
				return FieldType.BOOLEAN;
			case TINYINT:
			case SMALLINT:
			case INTEGER:
				return FieldType.INTEGER;
			case BIGINT:
				return FieldType.LONG;
			case REAL:
				return FieldType.FLOAT;
			case FLOAT:
			case DOUBLE:
				return FieldType.DOUBLE;
			case BINARY:
			case BLOB:
			case VARBINARY:
			case LONGVARBINARY:
				return FieldType.BLOB;
			case DATE:
			case TIME:
			case TIMESTAMP:
				return FieldType.DATETIME;
			default:
				return FieldType.STRING;
			}
		}
		return super.getFieldType();
	}

}
