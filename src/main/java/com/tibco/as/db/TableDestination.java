package com.tibco.as.db;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
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
	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String TABLE_CAT = "TABLE_CAT";
	private static final String TABLE_SCHEM = "TABLE_SCHEM";
	private static final String TABLE_TYPE = "TABLE_TYPE";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String COLUMN_DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private static final String KEY_SEQ = "KEY_SEQ";
	private static final String NULLABLE = "NULLABLE";
	private static final String COLUMN_NUM_PREC_RADIX = "NUM_PREC_RADIX";
	private static final String COLUMN_ORDINAL_POSITION = "ORDINAL_POSITION";

	private Logger log = LogFactory.getLog(TableDestination.class);
	private DatabaseChannel channel;
	private Table table;

	public TableDestination(DatabaseChannel channel, Table table) {
		super(channel);
		this.channel = channel;
		this.table = table;
	}

	@Override
	public TableDestination clone() {
		TableDestination destination = new TableDestination(channel, table);
		copyTo(destination);
		return destination;
	}

	@Override
	public void copyTo(Destination target) {
		TableDestination destination = (TableDestination) target;
		copy(table, destination.table);
		super.copyTo(destination);
	}

	public static void copy(Table source, Table target) {
		if (target.getCatalog() == null) {
			target.setCatalog(source.getCatalog());
		}
		if (target.getCountSQL() == null) {
			target.setCountSQL(source.getCountSQL());
		}
		if (target.getInsertSQL() == null) {
			target.setInsertSQL(source.getInsertSQL());
		}
		if (target.getName() == null) {
			target.setName(source.getName());
		}
		if (target.getSchema() == null) {
			target.setSchema(source.getSchema());
		}
		if (target.getSelectSQL() == null) {
			target.setSelectSQL(source.getSelectSQL());
		}
		if (target.getSpace() == null) {
			target.setSpace(source.getSpace());
		}
		if (target.getType() == null) {
			target.setType(source.getType());
		}
		for (Column sourceColumn : source.getColumns()) {
			Column targetColumn = getColumn(target, getColumnName(sourceColumn));
			if (targetColumn == null) {
				targetColumn = new Column();
				target.getColumns().add(targetColumn);
			}
			copy(sourceColumn, targetColumn);
		}
	}

	public static void copy(Column source, Column target) {
		if (target.getDecimals() == null) {
			target.setDecimals(source.getDecimals());
		}
		if (target.getField() == null) {
			target.setField(source.getField());
		}
		if (target.getKeySequence() == null) {
			target.setKeySequence(source.getKeySequence());
		}
		if (target.getName() == null) {
			target.setName(source.getName());
		}
		if (target.isNullable() == null) {
			target.setNullable(source.isNullable());
		}
		if (target.getRadix() == null) {
			target.setRadix(source.getRadix());
		}
		if (target.getSize() == null) {
			target.setSize(source.getSize());
		}
		if (target.getType() == null) {
			target.setType(source.getType());
		}
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
				Column column = getColumn(primaryKey);
				if (column == null) {
					continue;
				}
				keys.add(getFieldName(column));
			}
		}
		return keys;
	}

	private String getFieldName(Column column) {
		if (column.getField() == null) {
			return column.getName();
		}
		return column.getField();
	}

	private static Column getColumn(Table table, String columnName) {
		for (Column column : table.getColumns()) {
			if (columnName.equals(getColumnName(column))) {
				return column;
			}
		}
		return null;
	}

	public Column getColumn(String name) {
		return getColumn(table, name);
	}

	private static String getColumnName(Column column) {
		if (column.getName() == null) {
			return column.getField();
		}
		return column.getName();
	}

	public Collection<String> getPrimaryKeys() {
		Map<Short, String> keyMap = new TreeMap<Short, String>();
		for (Column column : table.getColumns()) {
			if (column.getKeySequence() == null) {
				continue;
			}
			keyMap.put(column.getKeySequence(), getColumnName(column));
		}
		if (keyMap.isEmpty()) {
			Collection<String> primaryKeys = new ArrayList<String>();
			for (String key : super.getKeys()) {
				Column column = getColumnByFieldName(key);
				if (column != null) {
					primaryKeys.add(getColumnName(column));
				}
			}
			return primaryKeys;
		}
		return keyMap.values();
	}

	private Column getColumnByFieldName(String fieldName) {
		for (Column column : table.getColumns()) {
			if (getFieldName(column).equals(fieldName)) {
				return column;
			}
		}
		return null;
	}

	public String getSelectSQL() {
		if (table.getSelectSQL() == null) {
			String from = getCommaSeparated(getColumnNames());
			return MessageFormat.format(SELECT, from, getFullyQualifiedName());
		}
		return table.getSelectSQL();
	}

	public String getCountSQL() {
		if (table.getCountSQL() == null && table.getSelectSQL() == null) {
			table.setCountSQL(MessageFormat.format(SELECT, "COUNT(*)",
					getFullyQualifiedName()));
		}
		return table.getCountSQL();
	}

	public String getInsertSQL() {
		if (table.getInsertSQL() == null) {
			String tableName = getFullyQualifiedName();
			String[] columnNames = getColumnNames();
			String[] questionMarks = new String[columnNames.length];
			Arrays.fill(questionMarks, "?");
			table.setInsertSQL(MessageFormat.format(
					"INSERT INTO {0} ({1}) VALUES ({2})", tableName,
					getCommaSeparated(columnNames),
					getCommaSeparated(questionMarks)));
		}
		return table.getInsertSQL();
	}

	public Table getTable() {
		return table;
	}

	public String getTableName() {
		return getTableName(table);
	}

	public TableType getType() {
		if (table.getType() == null) {
			return TableType.TABLE;
		}
		return table.getType();
	}

	@Override
	public String getSpaceName() {
		String spaceName = super.getSpaceName();
		if (spaceName == null) {
			if (table.getSpace() == null) {
				return table.getName();
			}
			return table.getSpace();
		}
		return spaceName;
	}

	public String getFullyQualifiedName() {
		String namespace = "";
		if (table.getCatalog() != null) {
			namespace += quote(table.getCatalog()) + ".";
		}
		if (table.getSchema() != null) {
			namespace += quote(table.getSchema()) + ".";
		}
		return namespace + quote(getTableName());
	}

	public String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public String[] getColumnNames() {
		if (table.getColumns().isEmpty()) {
			return new String[] { "*" };
		}
		Collection<String> columnNames = new ArrayList<String>();
		for (Column column : table.getColumns()) {
			columnNames.add(quote(getColumnName(column)));
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
		return getTableName();
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
		for (Column column : table.getColumns()) {
			String columnName = quote(getColumnName(column));
			JDBCType type = column.getType();
			String typeName = type.getName();
			query += columnName + " " + typeName;
			Integer size = column.getSize();
			if (size != null) {
				query += "(";
				query += size;
				if (column.getDecimals() != null) {
					query += "," + column.getDecimals();
				}
				query += ")";
			}
			if (!Boolean.TRUE.equals(column.isNullable())) {
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

	@Override
	public void setSpaceDef(SpaceDef spaceDef) {
		super.setSpaceDef(spaceDef);
		if (table.getColumns().isEmpty()) {
			for (String fieldName : getFieldNames()) {
				Column column = new Column();
				column.setField(fieldName);
				table.getColumns().add(column);
			}
		}
		for (FieldDef fieldDef : getFieldDefs()) {
			Column column = getColumnByFieldName(fieldDef.getName());
			if (column == null) {
				continue;
			}
			if (column.getField() == null) {
				column.setField(fieldDef.getName());
			}
			if (column.getType() == null) {
				column.setType(getColumnType(fieldDef.getType()));
			}
			if (column.isNullable() == null) {
				column.setNullable(fieldDef.isNullable());
			}
			if (column.getSize() == null) {
				column.setSize(getColumnSize(fieldDef.getType()));
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
		for (Column column : table.getColumns()) {
			result.add(getAccessor(index, column.getType()));
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

	public FieldType getFieldType(Column column) {
		switch (column.getType()) {
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
		Column column = getColumnByFieldName(fieldDef.getName());
		if (column == null || column.getType() == null) {
			return getJavaType(getColumnType(fieldDef.getType()));
		}
		return getJavaType(column.getType());
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
			for (Column column : table.getColumns()) {
				String fieldName = getFieldName(column);
				FieldType fieldType = getFieldType(column);
				FieldDef fieldDef = FieldDef.create(fieldName, fieldType);
				if (column.isNullable() != null) {
					fieldDef.setNullable(column.isNullable());
				}
				fieldDefs.add(fieldDef);
			}
		}
		return fieldDefs;
	}

	private DatabaseMetaData getMetaData() throws SQLException {
		return channel.getMetaData();
	}

	public Collection<Table> getTables() throws SQLException {
		String catalog = table.getCatalog();
		String schema = table.getSchema();
		String name = getTableName(table);
		String[] types = new String[] { getTableType(table).name() };
		log.log(Level.FINE,
				"Retrieving table descriptions matching catalog={0} schema={1} name={2} types={3}",
				new Object[] { catalog, schema, name, types });
		ResultSet rs = getMetaData().getTables(catalog, schema, name, types);
		Collection<Table> tables = new ArrayList<Table>();
		try {
			while (rs.next()) {
				Table t = new Table();
				t.setCountSQL(table.getCountSQL());
				t.setFetchSize(table.getFetchSize());
				t.setInsertSQL(table.getInsertSQL());
				t.setSelectSQL(table.getSelectSQL());
				t.setSpace(table.getSpace());
				t.setCatalog(rs.getString(TABLE_CAT));
				t.setName(rs.getString(TABLE_NAME));
				t.setSchema(rs.getString(TABLE_SCHEM));
				t.setType(TableType.valueOf(rs.getString(TABLE_TYPE)));
				Map<Integer, Column> map = new TreeMap<Integer, Column>();
				for (Column column : getColumns(table)) {
					ResultSet columnRS = getMetaData().getColumns(
							t.getCatalog(), t.getSchema(), getTableName(t),
							getColumnName(column));
					try {
						while (columnRS.next()) {
							Column c = new Column();
							c.setName(columnRS.getString(COLUMN_NAME));
							c.setField(column.getField());
							c.setDecimals(columnRS
									.getInt(COLUMN_DECIMAL_DIGITS));
							c.setNullable(columnRS.getBoolean(NULLABLE));
							c.setRadix(columnRS.getInt(COLUMN_NUM_PREC_RADIX));
							c.setSize(columnRS.getInt(COLUMN_SIZE));
							c.setType(JDBCType.valueOf(columnRS
									.getInt(COLUMN_DATA_TYPE)));
							map.put(columnRS.getInt(COLUMN_ORDINAL_POSITION), c);
						}
					} finally {
						columnRS.close();
					}
				}
				t.getColumns().addAll(map.values());
				ResultSet keyRS = getMetaData().getPrimaryKeys(t.getCatalog(),
						t.getSchema(), t.getName());
				try {
					while (keyRS.next()) {
						String columnName = keyRS.getString(COLUMN_NAME);
						Column column = getColumn(t, columnName);
						if (column == null) {
							continue;
						}
						column.setKeySequence(keyRS.getShort(KEY_SEQ));
					}
				} finally {
					keyRS.close();
				}
				tables.add(t);
			}
		} finally {
			rs.close();
		}
		return tables;
	}

	private String getTableName(Table table) {
		if (table.getName() == null) {
			return table.getSpace();
		}
		return table.getName();
	}

	private Collection<Column> getColumns(Table table) {
		if (table.getColumns().isEmpty()) {
			return Arrays.asList(new Column());
		}
		return table.getColumns();
	}

	private TableType getTableType(Table table) {
		if (table.getType() == null) {
			return TableType.TABLE;
		}
		return table.getType();
	}

	@Override
	public void setSpaceName(String spaceName) {
		table.setSpace(spaceName);
		super.setSpaceName(spaceName);
	}

}
