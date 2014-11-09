package com.tibco.as.db;

public class ColumnConfig {

	private String columnName;
	private JDBCType columnType;
	private Integer columnSize;
	private Integer decimalDigits;
	private Integer radix;
	private Boolean columnNullable;
	private Short keySequence;
	private String fieldName;

	public ColumnConfig clone() {
		ColumnConfig clone = new ColumnConfig();
		copyTo(clone);
		return clone;
	}

	public void copyTo(ColumnConfig target) {
		if (target.columnName == null) {
			target.columnName = columnName;
		}
		if (target.columnType == null) {
			target.columnType = columnType;
		}
		if (target.columnSize == null) {
			target.columnSize = columnSize;
		}
		if (target.decimalDigits == null) {
			target.decimalDigits = decimalDigits;
		}
		if (target.radix == null) {
			target.radix = radix;
		}
		if (target.columnNullable == null) {
			target.columnNullable = columnNullable;
		}
		if (target.keySequence == null) {
			target.keySequence = keySequence;
		}
		if (target.fieldName == null) {
			target.fieldName = fieldName;
		}
	}

	public Short getKeySequence() {
		return keySequence;
	}

	public void setKeySequence(Short keySequence) {
		this.keySequence = keySequence;
	}

	public String getColumnName() {
		if (columnName == null) {
			return fieldName;
		}
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public JDBCType getColumnType() {
		return columnType;
	}

	public void setColumnType(JDBCType columnType) {
		this.columnType = columnType;
	}

	public Integer getColumnSize() {
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
		return columnNullable;
	}

	public void setColumnNullable(Boolean columnNullable) {
		this.columnNullable = columnNullable;
	}

	public String getFieldName() {
		if (fieldName == null) {
			return columnName;
		}
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

}
