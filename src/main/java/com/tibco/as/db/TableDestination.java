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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.db.accessors.BlobAccessor;
import com.tibco.as.db.accessors.ClobAccessor;
import com.tibco.as.db.accessors.DateAccessor;
import com.tibco.as.db.accessors.IColumnAccessor;
import com.tibco.as.db.accessors.ObjectAccessor;
import com.tibco.as.db.accessors.TimeAccessor;
import com.tibco.as.db.accessors.TimestampAccessor;
import com.tibco.as.io.AbstractDestination;
import com.tibco.as.io.IStreamAdapter;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.convert.ConverterFactory;
import com.tibco.as.util.convert.IConverter;
import com.tibco.as.util.log.LogFactory;

public class TableDestination extends AbstractDestination<Object[]> {

	private static final String SELECT = "SELECT {0} FROM {1}";
	private final static char QUOTE = '\"';
	private static final int DEFAULT_BLOB_SIZE = 255;
	private static final int DEFAULT_CLOB_SIZE = 255;
	private static final int DEFAULT_BOOLEAN_SIZE = 1;
	private static final int DEFAULT_CHAR_SIZE = 1;
	private static final int DEFAULT_LONG_SIZE = 19;
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String COLUMN_DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private static final String KEY_SEQ = "KEY_SEQ";
	private static final String NULLABLE = "NULLABLE";
	private static final String COLUMN_NUM_PREC_RADIX = "NUM_PREC_RADIX";
	// private static final String COLUMN_ORDINAL_POSITION = "ORDINAL_POSITION";

	private Logger log = LogFactory.getLog(TableDestination.class);
	private DatabaseChannel channel;
	private Table table = new Table();

	public TableDestination(DatabaseChannel channel) {
		super(channel);
		this.channel = channel;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	private void setPrimaryKeys() throws SQLException {
		Map<String, Column> nameMap = new HashMap<String, Column>();
		for (Column column : table.getColumns()) {
			nameMap.put(column.getName(), column);
		}
		ResultSet keyRS = channel.getMetaData().getPrimaryKeys(
				table.getCatalog(), table.getSchema(), table.getName());
		try {
			while (keyRS.next()) {
				String columnName = keyRS.getString(COLUMN_NAME);
				Column column = nameMap.get(columnName);
				if (column == null) {
					continue;
				}
				column.setKeySequence(keyRS.getShort(KEY_SEQ));
			}
		} finally {
			keyRS.close();
		}
	}

	public void setColumns() throws SQLException {
		// Map<Integer, Column> positionMap = new TreeMap<Integer, Column>();
		ResultSet columnRS = channel.getMetaData().getColumns(
				table.getCatalog(), table.getSchema(), table.getName(), null);
		try {
			while (columnRS.next()) {
				String name = columnRS.getString(COLUMN_NAME);
				Column column = getColumn(name);
				column.setName(name);
				column.setDecimals(columnRS.getInt(COLUMN_DECIMAL_DIGITS));
				column.setNullable(columnRS.getBoolean(NULLABLE));
				column.setRadix(columnRS.getInt(COLUMN_NUM_PREC_RADIX));
				column.setSize(columnRS.getInt(COLUMN_SIZE));
				column.setType(JDBCType.valueOf(columnRS
						.getInt(COLUMN_DATA_TYPE)));
				// int position = columnRS.getInt(COLUMN_ORDINAL_POSITION);
				// positionMap.put(position, column);
			}
		} finally {
			columnRS.close();
		}
		setPrimaryKeys();
		// return positionMap.values();
	}

	@Override
	public DatabaseChannel getChannel() {
		return channel;
	}

	@Override
	public SpaceDef getSpaceDef() {
		SpaceDef spaceDef = super.getSpaceDef();
		if (spaceDef.getName() == null || spaceDef.getName().isEmpty()) {
			spaceDef.setName(getSpaceName());
		}
		Collection<FieldDef> fieldDefs = spaceDef.getFieldDefs();
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
		Collection<String> keys = spaceDef.getKeyDef().getFieldNames();
		if (keys.isEmpty()) {
			try {
				setPrimaryKeys();
			} catch (SQLException e) {
				log.log(Level.SEVERE,
						"Could not retrieve primary keys from metadata", e);
			}
			for (String primaryKey : getPrimaryKeys()) {
				Column column = getColumn(primaryKey);
				if (column == null) {
					continue;
				}
				keys.add(getFieldName(column));
			}
		}
		return spaceDef;
	}

	private String getSpaceName() {
		if (table.getSpace() == null) {
			return table.getName();
		}
		return table.getSpace();
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

	private Collection<String> getPrimaryKeys() {
		Map<Short, String> keyMap = new TreeMap<Short, String>();
		for (Column column : table.getColumns()) {
			if (column.getKeySequence() == null) {
				continue;
			}
			keyMap.put(column.getKeySequence(), getColumnName(column));
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

	public String getTableName() {
		return getTableName(table);
	}

	public TableType getType() {
		if (table.getType() == null) {
			return TableType.TABLE;
		}
		return table.getType();
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
	protected TableOutputStream getOutputStream() {
		return new TableOutputStream(this);
	}

	@Override
	public TableInputStream getInputStream() {
		return new TableInputStream(this);
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

	public String getCreateSQL() {
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
		if (table.getName() == null) {
			table.setName(spaceDef.getName());
		}
		if (table.getSpace() == null) {
			table.setSpace(spaceDef.getName());
		}
		int size = spaceDef.getFieldDefs().size();
		if (table.getColumns().isEmpty()) {
			for (int index = 0; index < size; index++) {
				table.getColumns().add(new Column());
			}
		}
		FieldDef[] fieldDefs = spaceDef.getFieldDefs().toArray(
				new FieldDef[size]);
		for (int index = 0; index < table.getColumns().size(); index++) {
			Column column = table.getColumns().get(index);
			if (column.getField() == null) {
				column.setField(fieldDefs[index].getName());
			}
			FieldDef fieldDef = spaceDef.getFieldDef(column.getField());
			if (fieldDef != null) {
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
		short keySequence = 1;
		for (String fieldName : spaceDef.getKeyDef().getFieldNames()) {
			Column column = getColumnByFieldName(fieldName);
			if (column == null) {
				continue;
			}
			column.setKeySequence(keySequence);
			keySequence++;
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

	//
	// private Integer getColumnSize(JDBCType jdbcType) {
	// switch (jdbcType) {
	// case BINARY:
	// case BLOB:
	// case LONGVARBINARY:
	// case VARBINARY:
	// return DEFAULT_BLOB_SIZE;
	// case CHAR:
	// case CLOB:
	// case LONGNVARCHAR:
	// case LONGVARCHAR:
	// case NCHAR:
	// case NCLOB:
	// case NVARCHAR:
	// case VARCHAR:
	// return DEFAULT_CLOB_SIZE;
	// default:
	// return null;
	// }
	// }

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

	public IColumnAccessor[] getColumnAccessors() {
		Collection<IColumnAccessor> result = new ArrayList<IColumnAccessor>();
		for (int index = 0; index < table.getColumns().size(); index++) {
			Column column = table.getColumns().get(index);
			result.add(getAccessor(index + 1, column.getType()));
		}
		return result.toArray(new IColumnAccessor[result.size()]);
	}

	private IColumnAccessor getAccessor(int index, JDBCType type) {
		switch (type) {
		case BLOB:
			return new BlobAccessor(index);
		case CLOB:
			return new ClobAccessor(index);
		case DATE:
			return new DateAccessor(index);
		case TIME:
			return new TimeAccessor(index);
		case TIMESTAMP:
			return new TimestampAccessor(index);
		default:
			return new ObjectAccessor(index, type.getType());
		}
	}

	public FieldType getFieldType(Column column) {
		if (column.getType() == null) {
			return null;
		}
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
			if (column.getDecimals() == null || column.getDecimals() > 0) {
				return FieldType.DOUBLE;
			}
			return FieldType.LONG;
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
	protected ObjectArrayStreamAdapter getInputStreamAdapter() {
		return new ObjectArrayStreamAdapter(this);
	}

	@Override
	protected IStreamAdapter<Tuple, Object[]> getOutputStreamAdapter() {
		return new TupleStreamAdapter(this);
	}

	public Class<?> getJavaType(JDBCType type) {
		switch (type) {
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

	private String getTableName(Table table) {
		if (table.getName() == null) {
			return table.getSpace();
		}
		return table.getName();
	}

	public IConverter[] getInputConverters() {
		ConverterFactory factory = getConverterFactory();
		IConverter[] converters = new IConverter[table.getColumns().size()];
		for (int index = 0; index < table.getColumns().size(); index++) {
			Column column = table.getColumns().get(index);
			FieldDef fieldDef = getSpaceDef().getFieldDef(getFieldName(column));
			Class<?> from = getJavaType(column.getType());
			FieldType to = fieldDef.getType();
			IConverter converter = factory.getConverter(from, to);
			converters[index] = converter;
		}
		return converters;
	}

	public IConverter[] getOutputConverters() {
		ConverterFactory factory = getConverterFactory();
		IConverter[] converters = new IConverter[table.getColumns().size()];
		for (int index = 0; index < table.getColumns().size(); index++) {
			Column column = table.getColumns().get(index);
			FieldDef fieldDef = getSpaceDef().getFieldDef(getFieldName(column));
			FieldType from = fieldDef.getType();
			Class<?> to = getJavaType(column.getType());
			IConverter converter = factory.getConverter(from, to);
			converters[index] = converter;
		}
		return converters;
	}
}
