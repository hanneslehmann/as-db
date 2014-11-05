package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.Destination;
import com.tibco.as.io.Field;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public class TableDestination extends Destination {

	private static final String SELECT = "SELECT {0} FROM {1}";
	private final static int DEFAULT_INSERT_BATCH_SIZE = 1000;
	private final static char QUOTE = '\"';

	private Logger log = LogFactory.getLog(TableDestination.class);
	private DatabaseChannel channel;
	private String selectSQL;
	private String countSQL;
	private String insertSQL;
	private String catalog;
	private String schema;
	private String table;
	private TableType type;
	private Integer fetchSize;
	private Integer tableBatchSize;

	public TableDestination(DatabaseChannel channel) {
		super(channel);
		this.channel = channel;
	}

	@Override
	public ColumnConfig addField() {
		return (ColumnConfig) super.addField();
	}

	@Override
	public TableDestination clone() {
		TableDestination destination = new TableDestination(channel);
		copyTo(destination);
		return destination;
	}

	@Override
	public void copyTo(Destination target) {
		TableDestination destination = (TableDestination) target;
		if (destination.catalog == null) {
			destination.catalog = catalog;
		}
		if (destination.countSQL == null) {
			destination.countSQL = countSQL;
		}
		if (destination.fetchSize == null) {
			destination.fetchSize = fetchSize;
		}
		if (destination.tableBatchSize == null) {
			destination.tableBatchSize = tableBatchSize;
		}
		if (destination.insertSQL == null) {
			destination.insertSQL = insertSQL;
		}
		if (destination.schema == null) {
			destination.schema = schema;
		}
		if (destination.selectSQL == null) {
			destination.selectSQL = selectSQL;
		}
		if (destination.table == null) {
			destination.table = table;
		}
		if (destination.type == null) {
			destination.type = type;
		}
		super.copyTo(destination);
	}

	@Override
	public DatabaseChannel getChannel() {
		return channel;
	}

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
		if (selectSQL == null) {
			String from = getCommaSeparated(getColumnNames());
			selectSQL = MessageFormat.format(SELECT, from,
					getFullyQualifiedName());
		}
		return selectSQL;
	}

	public void setSelectSQL(String selectSQL) {
		this.selectSQL = selectSQL;
	}

	public String getCountSQL() {
		if (countSQL == null && selectSQL == null) {
			countSQL = MessageFormat.format(SELECT, "COUNT(*)",
					getFullyQualifiedName());
		}
		return countSQL;
	}

	public void setCountSQL(String countSQL) {
		this.countSQL = countSQL;
	}

	public String getInsertSQL() {
		if (insertSQL == null) {
			String tableName = getFullyQualifiedName();
			String[] columnNames = getColumnNames();
			String[] questionMarks = new String[columnNames.length];
			Arrays.fill(questionMarks, "?");
			insertSQL = MessageFormat.format(
					"INSERT INTO {0} ({1}) VALUES ({2})", tableName,
					getCommaSeparated(columnNames),
					getCommaSeparated(questionMarks));
		}
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

	@Override
	public TableOutputStream getOutputStream() {
		int batchSize = getTableBatchSize();
		if (batchSize > 1) {
			return new BatchTableOutputStream(this, batchSize);
		}
		return new TableOutputStream(this);

	}

	@Override
	public IInputStream getInputStream() throws Exception {
		return new TableInputStream(this);
	}

	@Override
	public String getName() {
		return getTable();
	}

	private String getCommaSeparated(String[] elements) {
		String result = "";
		for (int index = 0; index < elements.length; index++) {
			if (index > 0) {
				result += ", ";
			}
			result += elements[index];
		}
		return result;
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		Statement statement = channel.getConnection().createStatement();
		log.log(Level.FINE, "Executing query: {0}", sql);
		try {
			return statement.executeQuery(sql);
		} catch (SQLException e) {
			statement.close();
			throw e;
		}
	}

	public String getCreateSQL() throws SQLException {
		String query = "";
		query += "CREATE TABLE " + getFullyQualifiedName() + " (";
		for (ColumnConfig column : getColumns()) {
			String columnName = quote(column.getColumnName());
			JDBCType type = column.getColumnType();
			String typeName = type.getName();
			query += columnName + " " + typeName;
			Integer size = column.getColumnSize();
			if (size != null) {
				query += "(";
				query += size;
				if (column.getDecimalDigits() != null) {
					query += "," + column.getDecimalDigits();
				}
				query += ")";
			}
			if (!Boolean.TRUE.equals(column.getColumnNullable())) {
				query += " not";
			}
			query += " null, ";
		}
		query += "Primary Key (";
		int index = 0;
		for (String key : getPrimaryKeys()) {
			if (index > 0) {
				query += ",";
			}
			query += quote(key);
			index++;
		}
		query += ")"; // close primary key constraint
		query += ")"; // close table def
		return query;
	}

	public void execute(String sql) throws SQLException {
		Statement statement = channel.getConnection().createStatement();
		log.log(Level.FINE, "Executing statement: {0}", sql);
		try {
			statement.execute(sql);
		} finally {
			statement.close();
		}
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return channel.getConnection().prepareStatement(sql);
	}

	public IPreparedStatementAccessor[] getAccessors() {
		Collection<IPreparedStatementAccessor> result = new ArrayList<IPreparedStatementAccessor>();
		List<ColumnConfig> columns = getColumns();
		for (int index = 0; index < columns.size(); index++) {
			ColumnConfig column = columns.get(index);
			result.add(getAccessor(index + 1, column.getColumnType()));
		}
		return result.toArray(new IPreparedStatementAccessor[result.size()]);
	}

	private IPreparedStatementAccessor getAccessor(int index, JDBCType type) {
		switch (type) {
		case BLOB:
			return new BlobAccessor(index);
		default:
			return new DefaultAccessor(index, type.getType());
		}
	}

}
