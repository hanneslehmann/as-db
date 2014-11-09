package com.tibco.as.db;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.Destination;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;

public class TableDestination extends Destination {

	private static final String SELECT = "SELECT {0} FROM {1}";
	private final static char QUOTE = '\"';
	private static final int DEFAULT_BLOB_SIZE = 255;
	private static final int DEFAULT_CLOB_SIZE = 255;
	private static final int DEFAULT_BOOLEAN_SIZE = 1;
	private static final int DEFAULT_CHAR_SIZE = 1;
	private static final int DEFAULT_LONG_SIZE = 19;

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
	private Collection<ColumnConfig> columns = new ArrayList<ColumnConfig>();

	public TableDestination(DatabaseChannel channel) {
		super(channel);
		this.channel = channel;
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
		for (ColumnConfig column : columns) {
			destination.columns.add(column.clone());
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
				ColumnConfig column = getColumn(primaryKey);
				if (column == null) {
					continue;
				}
				keys.add(column.getFieldName());
			}
		}
		return keys;
	}

	public ColumnConfig getColumn(String name) {
		for (ColumnConfig column : getColumns()) {
			if (name.equals(column.getColumnName())) {
				return column;
			}
		}
		return null;
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
				ColumnConfig column = getColumnByFieldName(key);
				if (column != null) {
					primaryKeys.add(column.getColumnName());
				}
			}
			return primaryKeys;
		}
		return keyMap.values();
	}

	private ColumnConfig getColumnByFieldName(String fieldName) {
		for (ColumnConfig column : columns) {
			if (column.getFieldName().equals(fieldName)) {
				return column;
			}
		}
		return null;
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
			return getSpaceName();
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

	@Override
	public String getSpaceName() {
		String spaceName = super.getSpaceName();
		if (spaceName == null) {
			return table;
		}
		return spaceName;
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

	@Override
	public TableOutputStream getOutputStream() {
		Integer batchSize = getExportConfig().getBatchSize();
		if (batchSize == null || batchSize == 1) {
			return new TableOutputStream(this);
		}
		return new BatchTableOutputStream(this, batchSize);
	}

	@Override
	public IInputStream getInputStream() {
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

	public Collection<ColumnConfig> getColumns() {
		return columns;
	}

	@Override
	public void setSpaceDef(SpaceDef spaceDef) {
		super.setSpaceDef(spaceDef);
		for (FieldDef fieldDef : getFieldDefs()) {
			ColumnConfig column = getColumnByFieldName(fieldDef.getName());
			if (column == null) {
				column = new ColumnConfig();
				columns.add(column);
			}
			column.setFieldName(fieldDef.getName());
			if (column.getColumnType() == null) {
				column.setColumnType(getColumnType(fieldDef.getType()));
			}
			if (column.getColumnNullable() == null) {
				column.setColumnNullable(fieldDef.isNullable());
			}
			if (column.getColumnSize() == null) {
				column.setColumnSize(getColumnSize(fieldDef.getType()));
			}
		}
	}

	private Integer getColumnSize(FieldType fieldType) {
		switch (fieldType) {
		case BOOLEAN:
			return DEFAULT_BOOLEAN_SIZE;
		case CHAR:
			return DEFAULT_CHAR_SIZE;
		case LONG:
			return DEFAULT_LONG_SIZE;
		case BLOB:
			return DEFAULT_BLOB_SIZE;
		case STRING:
			return DEFAULT_CLOB_SIZE;
		default:
			return null;
		}
	}

	private Integer getColumnSize(JDBCType jdbcType) {
		switch (jdbcType) {
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

	private JDBCType getColumnType(FieldType fieldType) {
		switch (fieldType) {
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
		return null;
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
		int index = 1;
		for (ColumnConfig column : columns) {
			result.add(getAccessor(index, column.getColumnType()));
			index++;
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

	public FieldType getFieldType(ColumnConfig column) {
		switch (column.getColumnType()) {
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

	@Override
	protected Class<?> getJavaType(FieldDef fieldDef) {
		ColumnConfig column = getColumnByFieldName(fieldDef.getName());
		if (column == null || column.getColumnType() == null) {
			return getJavaType(getColumnType(fieldDef.getType()));
		}
		return getJavaType(column.getColumnType());
	}

	public Class<?> getJavaType(JDBCType jdbcType) {
		switch (jdbcType) {
		case CHAR:
		case VARCHAR:
		case LONGVARCHAR:
			return String.class;
		case NUMERIC:
		case DECIMAL:
			return BigDecimal.class;
		case BIT:
		case BOOLEAN:
			return Boolean.class;
		case TINYINT:
		case SMALLINT:
			return Short.class;
		case INTEGER:
			return Integer.class;
		case BIGINT:
			return Long.class;
		case REAL:
			return Float.class;
		case FLOAT:
		case DOUBLE:
			return Double.class;
		case BINARY:
		case VARBINARY:
		case LONGVARBINARY:
			return byte[].class;
		case DATE:
			return Date.class;
		case TIME:
			return Time.class;
		case TIMESTAMP:
			return Timestamp.class;
		case CLOB:
			return Clob.class;
		case BLOB:
			return byte[].class;
		default:
			return Object.class;
		}
	}

	@Override
	protected Collection<FieldDef> getFieldDefs() {
		Collection<FieldDef> fieldDefs = super.getFieldDefs();
		if (fieldDefs.isEmpty()) {
			for (ColumnConfig column : columns) {
				String fieldName = column.getFieldName();
				FieldType fieldType = getFieldType(column);
				FieldDef fieldDef = FieldDef.create(fieldName, fieldType);
				if (column.getColumnNullable() != null) {
					fieldDef.setNullable(column.getColumnNullable());
				}
				fieldDefs.add(fieldDef);
			}
		}
		return fieldDefs;
	}

	public void setColumns(Collection<ColumnConfig> columns) {
		this.columns = columns;
	}

}
