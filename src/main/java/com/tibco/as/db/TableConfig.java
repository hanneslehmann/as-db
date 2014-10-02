package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.io.DestinationConfig;

public class TableConfig extends DestinationConfig {

	private String selectSQL;
	private String countSQL;
	private String catalog;
	private String schema;
	private String table;
	private TableType type;
	private Integer fetchSize;
	private Integer insertBatchSize;
	private Collection<String> primaryKeys;

	public Collection<String> getPrimaryKeys() {
		if (primaryKeys == null) {
			Collection<String> primaryKeys = new ArrayList<String>();
			for (String key : getKeys()) {
				ColumnConfig column = (ColumnConfig) getField(key);
				primaryKeys.add(column.getColumnName());
			}
			return primaryKeys;
		}
		return primaryKeys;
	}

	@Override
	public Collection<String> getKeys() {
		Collection<String> keys = super.getKeys();
		if (keys == null) {
			return getPrimaryKeys();
		}
		return keys;
	}

	public void setPrimaryKeys(Collection<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public String getSelectSQL() {
		return selectSQL;
	}

	public void setSelectSQL(String selectSQL) {
		this.selectSQL = selectSQL;
	}

	public String getCountSQL() {
		return countSQL;
	}

	public void setCountSQL(String countSQL) {
		this.countSQL = countSQL;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		if (table == null) {
			return getSpace();
		}
		return table;
	}

	public void setTable(String name) {
		this.table = name;
	}

	public TableType getType() {
		return type;
	}

	public void setType(TableType type) {
		this.type = type;
	}

	public Integer getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
	}

	public Integer getInsertBatchSize() {
		return insertBatchSize;
	}

	public void setInsertBatchSize(Integer insertBatchSize) {
		this.insertBatchSize = insertBatchSize;
	}

	@Override
	public TableConfig clone() {
		TableConfig export = new TableConfig();
		copyTo(export);
		return export;
	}

	public void copyTo(TableConfig target) {
		target.catalog = catalog;
		target.countSQL = countSQL;
		target.fetchSize = fetchSize;
		target.insertBatchSize = insertBatchSize;
		target.primaryKeys = primaryKeys == null ? null
				: new ArrayList<String>(primaryKeys);
		target.schema = schema;
		target.selectSQL = selectSQL;
		target.table = table;
		target.type = type;
		super.copyTo(target);
	}

	@Override
	public String getSpace() {
		String space = super.getSpace();
		if (space == null) {
			return table;
		}
		return space;
	}

	@Override
	public ColumnConfig createFieldConfig() {
		return new ColumnConfig();
	}

}
