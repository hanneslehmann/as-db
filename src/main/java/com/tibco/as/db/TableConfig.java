package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tibco.as.convert.Field;
import com.tibco.as.io.DestinationConfig;

public class TableConfig extends DestinationConfig {

	private final static int DEFAULT_INSERT_BATCH_SIZE = 1000;
	private final static char QUOTE = '\"';

	private String selectSQL;
	private String countSQL;
	private String insertSQL;
	private String catalog;
	private String schema;
	private String table;
	private TableType type;
	private Integer fetchSize;
	private Integer tableBatchSize;

	@Override
	public Collection<String> getKeys() {
		Collection<String> keys = super.getKeys();
		if (keys.isEmpty()) {
			for (String primaryKey : getPrimaryKeys()) {
				keys.add(getColumn(primaryKey).getFieldName());
			}
		}
		return keys;
	}

	public ColumnConfig getColumn(String columnName) {
		for (ColumnConfig column : getColumns()) {
			if (columnName.equals(column.getColumnName())) {
				return column;
			}
		}
		ColumnConfig column = (ColumnConfig) addField();
		column.setColumnName(columnName);
		return column;
	}

	public Collection<String> getPrimaryKeys() {
		Map<Short, String> keyMap = new TreeMap<Short, String>();
		for (ColumnConfig column : getColumns()) {
			if (column.getKeySequence() == null) {
				continue;
			}
			keyMap.put(column.getKeySequence(), column.getColumnName());
		}
		if (keyMap.isEmpty()) {
			Collection<String> primaryKeys = new ArrayList<String>();
			for (String key : super.getKeys()) {
				ColumnConfig column = (ColumnConfig) getField(key);
				primaryKeys.add(column.getColumnName());
			}
			return primaryKeys;
		}
		return keyMap.values();
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

	public String getInsertSQL() {
		return insertSQL;
	}

	public void setInsertSQL(String insertSQL) {
		this.insertSQL = insertSQL;
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
		if (type == null) {
			return TableType.TABLE;
		}
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

	public int getTableBatchSize() {
		if (tableBatchSize == null) {
			return DEFAULT_INSERT_BATCH_SIZE;
		}
		return tableBatchSize;
	}

	public void setTableBatchSize(Integer batchSize) {
		this.tableBatchSize = batchSize;
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
		target.tableBatchSize = tableBatchSize;
		target.insertSQL = insertSQL;
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
	protected ColumnConfig newField() {
		return new ColumnConfig();
	}

	public String getFullyQualifiedName() {
		String namespace = "";
		if (getCatalog() != null) {
			namespace += quote(getCatalog()) + ".";
		}
		if (getSchema() != null) {
			namespace += quote(getSchema()) + ".";
		}
		return namespace + quote(getTable());
	}

	public String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public String[] getColumnNames() {
		Collection<ColumnConfig> columns = getColumns();
		if (columns.isEmpty()) {
			return new String[] { "*" };
		}
		Collection<String> columnNames = new ArrayList<String>();
		for (ColumnConfig column : columns) {
			columnNames.add(quote(column.getColumnName()));
		}
		return columnNames.toArray(new String[columnNames.size()]);
	}

	public List<ColumnConfig> getColumns() {
		List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
		for (Field field : getFields()) {
			columns.add((ColumnConfig) field);
		}
		return columns;
	}

}
