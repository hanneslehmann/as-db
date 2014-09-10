package com.tibco.as.db;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.db.accessors.BlobAccessor;
import com.tibco.as.db.accessors.DefaultAccessor;
import com.tibco.as.space.FieldDef.FieldType;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class DatabaseConnection {

	private static final String TABLE_NAME = "TABLE_NAME";

	private static final String TABLE_CAT = "TABLE_CAT";

	private static final String TABLE_SCHEM = "TABLE_SCHEM";

	private static final String TABLE_TYPE = "TABLE_TYPE";

	private static final String COLUMN_NAME = "COLUMN_NAME";

	private static final String DATA_TYPE = "DATA_TYPE";

	private static final String COLUMN_SIZE = "COLUMN_SIZE";

	private static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";

	private static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";

	private static final String NULLABLE = "NULLABLE";

	private static final String ORDINAL_POSITION = "ORDINAL_POSITION";

	private static final String KEY_SEQ = "KEY_SEQ";

	private static final String TYPE_NAME = "TYPE_NAME";

	private static final char QUOTE = '\"';

	private Connection connection;

	public DatabaseConnection(Database database) throws ClassNotFoundException,
			SQLException, InstantiationException, IllegalAccessException,
			MalformedURLException {
		if (database.getDriver() == null) {
			connection = DriverManager.getConnection(database.getUrl(),
					database.getUser(), database.getPassword());
		} else {
			Class<Driver> driverClass;
			if (database.getJar() == null) {
				driverClass = (Class<Driver>) Class.forName(database
						.getDriver());
			} else {
				URLClassLoader classLoader;
				URL url = new URL("file://" + database.getJar());
				classLoader = URLClassLoader.newInstance(new URL[] { url });
				driverClass = (Class<Driver>) classLoader.loadClass(database
						.getDriver());
			}
			Driver driver = driverClass.newInstance();
			Properties props = new Properties();
			if (database.getUser() != null) {
				props.put("user", database.getUser());
			}
			if (database.getPassword() != null) {
				props.put("password", database.getPassword());
			}
			connection = driver.connect(database.getUrl(), props);
		}
	}

	public String getFullyQualifiedName(Table table) {
		String namespace = "";
		if (table.getCatalog() != null) {
			namespace += quote(table.getCatalog()) + ".";
		}
		if (table.getSchema() != null) {
			namespace += quote(table.getSchema()) + ".";
		}
		return namespace + quote(table.getName());
	}

	private String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public String[] getColumnNames(Table table) {
		List<Column> columns = table.getColumns();
		String[] columnNames = new String[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			columnNames[index] = getName(columns.get(index));
		}
		return columnNames;
	}

	public String getName(Column column) {
		if (column.getName() == null) {
			return quote(column.getField());
		}
		return quote(column.getName());
	}

	public String getCommaSeparated(String[] elements) {
		String result = "";
		for (int index = 0; index < elements.length; index++) {
			if (index > 0) {
				result += ", ";
			}
			result += elements[index];
		}
		return result;
	}

	private List<Column> getColumns(String catalog, String schema,
			String table, String columnName) throws SQLException {
		Map<String, Short> keys = new HashMap<String, Short>();
		ResultSet keyRS = getMetaData().getPrimaryKeys(catalog, schema, table);
		try {
			while (keyRS.next()) {
				String key = keyRS.getString(COLUMN_NAME);
				short sequence = keyRS.getShort(KEY_SEQ);
				keys.put(key, sequence);
			}
		} finally {
			keyRS.close();
		}
		ResultSet columnRS = getMetaData().getColumns(catalog, schema, table,
				columnName);
		List<Column> columns = new ArrayList<Column>();
		try {
			while (columnRS.next()) {
				Column column = new Column();
				column.setDecimalDigits(columnRS.getInt(DECIMAL_DIGITS));
				column.setKeySequence(keys.get(columnRS.getString(COLUMN_NAME)));
				column.setName(columnRS.getString(COLUMN_NAME));
				column.setNullable(columnRS.getBoolean(NULLABLE));
				column.setRadix(columnRS.getInt(NUM_PREC_RADIX));
				column.setSize(columnRS.getInt(COLUMN_SIZE));
				column.setType(JDBCType.valueOf(columnRS.getInt(DATA_TYPE)));
				int position = columnRS.getInt(ORDINAL_POSITION);
				for (int index = columns.size(); index < position; index++) {
					columns.add(null);
				}
				columns.set(position - 1, column);
			}
		} finally {
			columnRS.close();
		}
		return columns;
	}

	public Collection<String> getSchemas(String catalog, String schema)
			throws SQLException {
		Collection<String> schemas = new ArrayList<String>();
		ResultSet rs = getMetaData().getSchemas(catalog, schema);
		try {
			while (rs.next()) {
				schemas.add(rs.getString(TABLE_SCHEM));
			}
		} finally {
			rs.close();
		}
		return schemas;
	}

	public Collection<String> getCatalogs() throws SQLException {
		Collection<String> catalogs = new ArrayList<String>();
		ResultSet rs = getMetaData().getCatalogs();
		try {
			while (rs.next()) {
				catalogs.add(rs.getString(TABLE_CAT));
			}
		} finally {
			rs.close();
		}
		return catalogs;
	}

	public List<Table> getTables(Table table) throws SQLException {
		List<Table> tables = getTables(table.getCatalog(), table.getSchema(),
				table.getName(), table.getType());
		for (Table t : tables) {
			t.setBatchSize(table.getBatchSize());
			t.setDistributionRole(table.getDistributionRole());
			t.setFetchSize(table.getFetchSize());
			t.setSpace(table.getSpace());
			t.setSql(table.getSql());
			t.getColumns().addAll(getColumns(t));
		}
		return tables;
	}

	public List<Column> getColumns(Table table) throws SQLException {
		List<Column> columns = new ArrayList<Column>();
		for (Column column : getTableColumns(table)) {
			for (Column found : getColumns(table.getCatalog(),
					table.getSchema(), table.getName(), column.getName())) {
				found.setField(column.getField());
				columns.add(found);
			}
		}
		return columns;
	}

	private List<Column> getTableColumns(Table table) {
		if (table.getColumns().isEmpty()) {
			return Arrays.asList(new Column());
		}
		return table.getColumns();
	}

	public List<Table> getTables(String catalog, String schema, String name,
			TableType... types) throws SQLException {
		List<Table> tables = new ArrayList<Table>();
		ResultSet rs = getMetaData().getTables(catalog, schema, name,
				getTypeNames(types));
		try {
			while (rs.next()) {
				Table table = new Table();
				table.setCatalog(rs.getString(TABLE_CAT));
				table.setName(rs.getString(TABLE_NAME));
				table.setSchema(rs.getString(TABLE_SCHEM));
				table.setType(TableType.valueOf(rs.getString(TABLE_TYPE)));
				tables.add(table);
			}
		} finally {
			rs.close();
		}
		return tables;
	}

	private String[] getTypeNames(TableType[] types) {
		String[] names = new String[types.length];
		for (int index = 0; index < types.length; index++) {
			names[index] = types[index].name();
		}
		return names;
	}

	public JDBCType getJDBCType(FieldType type) {
		switch (type) {
		case BLOB:
			return JDBCType.BLOB;
		case BOOLEAN:
			return JDBCType.BOOLEAN;
		case CHAR:
			return JDBCType.CHAR;
		case DATETIME:
			return JDBCType.TIMESTAMP;
		case DOUBLE:
			return JDBCType.DOUBLE;
		case FLOAT:
			return JDBCType.FLOAT;
		case INTEGER:
			return JDBCType.INTEGER;
		case LONG:
			return JDBCType.BIGINT;
		case SHORT:
			return JDBCType.SMALLINT;
		default:
			return JDBCType.VARCHAR;
		}
	}

	public Class getType(JDBCType type) {
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

	public Map<Integer, String> getTypeInfo() throws SQLException {
		Map<Integer, String> types = new HashMap<Integer, String>();
		ResultSet typeRS = getMetaData().getTypeInfo();
		while (typeRS.next()) {
			String typeName = typeRS.getString(TYPE_NAME);
			Integer dataType = typeRS.getInt(DATA_TYPE);
			types.put(dataType, typeName);
		}
		return types;
	}

	private DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	public IPreparedStatementAccessor[] getAccessors(Table table) {
		IPreparedStatementAccessor[] accessors = new IPreparedStatementAccessor[table
				.getColumns().size()];
		for (int index = 0; index < accessors.length; index++) {
			Column column = table.getColumns().get(index);
			accessors[index] = getAccessor(index + 1, column.getType()
					.getType());
		}
		return accessors;
	}

	private IPreparedStatementAccessor getAccessor(int index, int type) {
		switch (type) {
		case Types.BLOB:
			return new BlobAccessor(index);
		default:
			return new DefaultAccessor(index, type);
		}
	}

	public void close() throws SQLException {
		connection.close();
	}

	private void execute(String sql) throws SQLException {
		Statement statement = createStatement();
		try {
			statement.execute(sql);
		} finally {
			statement.close();
		}
	}

	private ResultSet executeQuery(String sql) throws SQLException {
		return createStatement().executeQuery(sql);
	}

	public PreparedStatement prepare(String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}

	private Statement createStatement() throws SQLException {
		return connection.createStatement();
	}

	private String getCreateSQL(Table table) throws SQLException {
		String query = "";
		query += "CREATE TABLE " + getFullyQualifiedName(table) + " (";
		String[] keys = new String[0];
		for (Column column : table.getColumns()) {
			String columnName = getName(column);
			JDBCType type = column.getType();
			String typeName = type.getName();
			query += columnName + " " + typeName;
			if (!Boolean.TRUE.equals(column.isNullable())) {
				query += " not";
			}
			query += " null, ";
			Short keySequence = column.getKeySequence();
			if (keySequence != null) {
				if (keys.length < keySequence) {
					keys = Arrays.copyOf(keys, keySequence);
				}
				keys[keySequence - 1] = columnName;
			}
		}
		query += "Primary Key (";
		for (int index = 0; index < keys.length; index++) {
			if (index > 0) {
				query += ",";
			}
			query += keys[index];
		}
		query += ")"; // close primary key constraint
		query += ")"; // close table def
		return query;
	}

	public void create(Table table) throws SQLException {
		if (table.getCatalog() != null) {
			if (!getCatalogs().contains(table.getCatalog())) {
				execute(MessageFormat.format("CREATE CATALOG \"{0}\"",
						table.getCatalog()));
			}
		}
		if (table.getSchema() != null) {
			if (!getSchemas(table.getCatalog(), null).contains(
					table.getSchema())) {
				execute(MessageFormat.format("CREATE SCHEMA \"{0}\"",
						table.getSchema()));
			}
		}
		String sql = getCreateSQL(table);
		execute(sql);
	}

	public PreparedStatement getInsertStatement(Table table)
			throws SQLException {
		String tableName = getFullyQualifiedName(table);
		String[] columnNames = getColumnNames(table);
		String[] questionMarks = new String[columnNames.length];
		Arrays.fill(questionMarks, "?");
		String sql = MessageFormat.format("INSERT INTO {0} ({1}) VALUES ({2})",
				tableName, getCommaSeparated(columnNames),
				getCommaSeparated(questionMarks));
		return prepare(sql);
	}

	public FieldType getFieldType(JDBCType type, int size, int decimalDigits) {
		switch (type) {
		case BIGINT:
			return FieldType.LONG;
		case BIT:
			return FieldType.BOOLEAN;
		case BLOB:
			return FieldType.BLOB;
		case BOOLEAN:
			return FieldType.BOOLEAN;
		case CHAR:
			return getStringFieldType(size);
		case CLOB:
			return getStringFieldType(size);
		case DATE:
			return FieldType.DATETIME;
		case DECIMAL:
			return getNumericalFieldType(size, decimalDigits);
		case DOUBLE:
			return FieldType.DOUBLE;
		case FLOAT:
			return FieldType.FLOAT;
		case INTEGER:
			return FieldType.INTEGER;
		case LONGNVARCHAR:
			return getStringFieldType(size);
		case LONGVARBINARY:
			return FieldType.BLOB;
		case LONGVARCHAR:
			return getStringFieldType(size);
		case NCHAR:
			return getStringFieldType(size);
		case NCLOB:
			return getStringFieldType(size);
		case NUMERIC:
			return getNumericalFieldType(size, decimalDigits);
		case NVARCHAR:
			return getStringFieldType(size);
		case REAL:
			return getNumericalFieldType(size, decimalDigits);
		case SMALLINT:
			return FieldType.SHORT;
		case SQLXML:
			return getStringFieldType(size);
		case TIME:
			return FieldType.DATETIME;
		case TIMESTAMP:
			return FieldType.DATETIME;
		case TINYINT:
			return FieldType.SHORT;
		case VARBINARY:
			return FieldType.BLOB;
		case VARCHAR:
			return getStringFieldType(size);
		default:
			return FieldType.STRING;
		}
	}

	private FieldType getStringFieldType(int size) {
		if (size == 1) {
			return FieldType.CHAR;
		}
		return FieldType.STRING;
	}

	private FieldType getNumericalFieldType(int size, int decimalDigits) {
		if (decimalDigits == 0) {
			if (size > AccessorFactory.INTEGER_SIZE) {
				return FieldType.LONG;
			}
			if (size > AccessorFactory.SHORT_SIZE) {
				return FieldType.INTEGER;
			}
			return FieldType.SHORT;
		}
		if (size > AccessorFactory.FLOAT_SIZE) {
			return FieldType.DOUBLE;
		}
		return FieldType.FLOAT;
	}

	public FieldType getFieldType(Column column) {
		JDBCType type = column.getType();
		int size = getInt(column.getSize());
		int decimalDigits = getInt(column.getDecimalDigits());
		return getFieldType(type, size, decimalDigits);
	}

	private int getInt(Integer integer) {
		if (integer == null) {
			return 0;
		}
		return integer;
	}

	public Connection getConnection() {
		return connection;
	}

	public ResultSet select(Table table) throws SQLException {
		if (table.getSql() == null) {
			String name = getFullyQualifiedName(table);
			String[] columnNames = getColumnNames(table);
			String names = getCommaSeparated(columnNames);
			String sql = MessageFormat.format("SELECT {0} FROM {1}", names,
					name);
			table.setSql(sql);
		}
		ResultSet resultSet = executeQuery(table.getSql());
		if (table.getFetchSize() != null) {
			resultSet.setFetchSize(table.getFetchSize());
		}
		// resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
		return resultSet;
	}

	public Map<String, Column> getColumns(ResultSet resultSet)
			throws SQLException {
		Map<String, Column> columns = new LinkedHashMap<String, Column>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		for (int index = 0; index < metaData.getColumnCount(); index++) {
			Column column = new Column();
			int columnIndex = index + 1;
			column.setDecimalDigits(metaData.getScale(columnIndex));
			column.setName(metaData.getColumnLabel(columnIndex));
			column.setNullable(metaData.isNullable(columnIndex) == ResultSetMetaData.columnNullable);
			column.setSize(metaData.getPrecision(columnIndex));
			column.setType(JDBCType.valueOf(metaData.getColumnType(columnIndex)));
			columns.put(column.getName(), column);
		}
		return columns;
	}

}
