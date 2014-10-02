package com.tibco.as.db;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.db.accessors.BlobAccessor;
import com.tibco.as.db.accessors.DefaultAccessor;
import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.FieldConfig;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public class DatabaseChannel extends AbstractChannel {

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
	private static final String SELECT = "SELECT {0} FROM {1}";

	private Logger log = LogFactory.getLog(DatabaseChannel.class);
	private DatabaseConfig config;
	private Connection connection;

	public DatabaseChannel(DatabaseConfig config) {
		super(config);
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		log.log(Level.INFO, "Opening connection to database {0}",
				config.getURL());
		connection = getConnection();
		connection.setAutoCommit(true);
		super.open();
	}

	private Connection getConnection() throws Exception {
		Properties props = new Properties();
		if (config.getUser() != null) {
			props.put("user", config.getUser());
		}
		if (config.getPassword() != null) {
			props.put("password", config.getPassword());
		}
		if (config.getDriver() == null) {
			return DriverManager.getConnection(config.getURL(), props);
		}
		Class<Driver> driverClass = getDriverClass();
		Driver driver = driverClass.newInstance();
		return driver.connect(config.getURL(), props);
	}

	@SuppressWarnings("unchecked")
	private Class<Driver> getDriverClass() throws MalformedURLException,
			ClassNotFoundException {
		if (config.getJar() == null) {
			return (Class<Driver>) Class.forName(config.getDriver());
		}
		URL[] urls = { new URL("file://" + config.getJar()) };
		URLClassLoader classLoader = URLClassLoader.newInstance(urls);
		log.log(Level.FINE, "Loading driver {0} from {1}", new Object[] {
				config.getDriver(), Arrays.toString(urls) });
		return (Class<Driver>) classLoader.loadClass(config.getDriver());
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (connection == null) {
			return;
		}
		log.info("Closing database connection");
		connection.close();
		connection = null;
	}

	public String getFullyQualifiedName(TableConfig table) {
		String namespace = "";
		if (table.getCatalog() != null) {
			namespace += quote(table.getCatalog()) + ".";
		}
		if (table.getSchema() != null) {
			namespace += quote(table.getSchema()) + ".";
		}
		return namespace + quote(table.getTable());
	}

	private String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public String[] getColumnNames(TableConfig table) {
		Collection<String> columnNames = new ArrayList<String>();
		for (FieldConfig field : table.getFields()) {
			ColumnConfig column = (ColumnConfig) field;
			columnNames.add(quote(column.getColumnName()));
		}
		return columnNames.toArray(new String[columnNames.size()]);
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

	private Collection<String> getKeys(TableConfig table) throws SQLException {
		Map<Short, String> keys = new TreeMap<Short, String>();
		ResultSet keyRS = getMetaData().getPrimaryKeys(table.getCatalog(),
				table.getSchema(), table.getTable());
		try {
			while (keyRS.next()) {
				String key = keyRS.getString(COLUMN_NAME);
				short sequence = keyRS.getShort(KEY_SEQ);
				keys.put(sequence, key);
			}
		} finally {
			keyRS.close();
		}
		return keys.values();
	}

	private Collection<ColumnConfig> getColumns(TableConfig table,
			ColumnConfig column) throws SQLException {
		ResultSet columnRS = getMetaData().getColumns(table.getCatalog(),
				table.getSchema(), table.getTable(), column.getColumnName());
		Map<Integer, ColumnConfig> columns = new TreeMap<Integer, ColumnConfig>();
		try {
			while (columnRS.next()) {
				ColumnConfig found = column.clone();
				found.setDecimalDigits(columnRS.getInt(DECIMAL_DIGITS));
				found.setColumnName(columnRS.getString(COLUMN_NAME));
				found.setColumnNullable(columnRS.getBoolean(NULLABLE));
				found.setRadix(columnRS.getInt(NUM_PREC_RADIX));
				found.setColumnSize(columnRS.getInt(COLUMN_SIZE));
				int dataType = columnRS.getInt(DATA_TYPE);
				found.setColumnType(JDBCType.valueOf(dataType));
				columns.put(columnRS.getInt(ORDINAL_POSITION), found);
			}
		} finally {
			columnRS.close();
		}
		return columns.values();
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

	public void populate(TableConfig table) throws SQLException {
		for (FieldConfig field : getTableColumns(table)) {
			table.getFields().addAll(getColumns(table, (ColumnConfig) field));
		}
		table.setPrimaryKeys(getKeys(table));
	}

	private List<FieldConfig> getTableColumns(TableConfig table) {
		if (table.getFields().isEmpty()) {
			return Arrays.asList((FieldConfig) new ColumnConfig());
		}
		return table.getFields();
	}

	private String[] getTypeNames(TableType... types) {
		String[] names = new String[types.length];
		for (int index = 0; index < types.length; index++) {
			names[index] = types[index].name();
		}
		return names;
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

	public IPreparedStatementAccessor[] getAccessors(TableConfig table) {
		IPreparedStatementAccessor[] accessors = new IPreparedStatementAccessor[table
				.getFields().size()];
		for (int index = 0; index < accessors.length; index++) {
			ColumnConfig column = (ColumnConfig) table.getFields().get(index);
			accessors[index] = getAccessor(index + 1, column.getColumnType()
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

	private String getCreateSQL(TableConfig table) throws SQLException {
		String query = "";
		query += "CREATE TABLE " + getFullyQualifiedName(table) + " (";
		for (FieldConfig field : table.getFields()) {
			ColumnConfig column = (ColumnConfig) field;
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
		for (String key : table.getPrimaryKeys()) {
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

	public void create(TableConfig table) throws SQLException {
		String sql = getCreateSQL(table);
		Statement statement = connection.createStatement();
		log.log(Level.FINE, "Executing ''{0}''", sql);
		try {
			statement.execute(sql);
		} finally {
			statement.close();
		}

	}

	public PreparedStatement getInsertStatement(TableConfig table)
			throws SQLException {
		String tableName = getFullyQualifiedName(table);
		String[] columnNames = getColumnNames(table);
		String[] questionMarks = new String[columnNames.length];
		Arrays.fill(questionMarks, "?");
		return prepare("INSERT INTO {0} ({1}) VALUES ({2})", tableName,
				getCommaSeparated(columnNames),
				getCommaSeparated(questionMarks));
	}

	private PreparedStatement prepare(String pattern, Object... arguments)
			throws SQLException {
		String sql = MessageFormat.format(pattern, arguments);
		log.log(Level.FINE, "Preparing statement ''{0}''", sql);
		return connection.prepareStatement(sql);
	}

	// private FieldType getStringFieldType(int size) {
	// if (size == 1) {
	// return FieldType.CHAR;
	// }
	// return FieldType.STRING;
	// }
	//
	// private FieldType getNumericalFieldType(int size, int decimalDigits) {
	// if (decimalDigits == 0) {
	// if (size > AccessorFactory.INTEGER_SIZE) {
	// return FieldType.LONG;
	// }
	// if (size > AccessorFactory.SHORT_SIZE) {
	// return FieldType.INTEGER;
	// }
	// return FieldType.SHORT;
	// }
	// if (size > AccessorFactory.FLOAT_SIZE) {
	// return FieldType.DOUBLE;
	// }
	// return FieldType.FLOAT;
	// }
	//
	// private int getInt(Integer integer) {
	// if (integer == null) {
	// return 0;
	// }
	// return integer;
	// }

	public ResultSet select(TableConfig table) throws SQLException {
		if (table.getSelectSQL() == null) {
			String[] columnNames = getColumnNames(table);
			String names = getCommaSeparated(columnNames);
			String name = getFullyQualifiedName(table);
			String selectSQL = MessageFormat.format(SELECT, names, name);
			table.setSelectSQL(selectSQL);
			if (table.getCountSQL() == null) {
				String countSQL = MessageFormat
						.format(SELECT, "COUNT(*)", name);
				table.setCountSQL(countSQL);
			}
		}
		ResultSet resultSet = executeQuery(table.getSelectSQL());
		// resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
		return resultSet;
	}

	public long getCount(TableConfig table) throws SQLException {
		String sql = table.getCountSQL();
		if (sql == null) {
			return IInputStream.UNKNOWN_SIZE;
		}
		ResultSet resultSet = executeQuery(sql);
		try {
			if (resultSet.next()) {
				return resultSet.getLong(1);
			}
			return IInputStream.UNKNOWN_SIZE;
		} finally {
			try {
				resultSet.close();
			} finally {
				resultSet.getStatement().close();
			}
		}
	}

	private ResultSet executeQuery(String sql) throws SQLException {
		Statement statement = connection.createStatement();
		log.log(Level.FINE, "Executing query ''{0}''", sql);
		try {
			return statement.executeQuery(sql);
		} catch (SQLException e) {
			statement.close();
			throw e;
		}
	}

	@Override
	protected IDestination createDestination(DestinationConfig config) {
		return new TableDestination(this, (TableConfig) config);
	}

	@Override
	protected Collection<DestinationConfig> getImportConfigs(
			DestinationConfig config) throws Exception {
		Collection<DestinationConfig> configs = new ArrayList<DestinationConfig>();
		TableConfig table = (TableConfig) config;
		ResultSet resultSet = getTables(table);
		try {
			while (resultSet.next()) {
				TableConfig found = table.clone();
				found.setCatalog(resultSet.getString(TABLE_CAT));
				found.setTable(resultSet.getString(TABLE_NAME));
				found.setSchema(resultSet.getString(TABLE_SCHEM));
				found.setType(TableType.valueOf(resultSet.getString(TABLE_TYPE)));
				configs.add(found);
			}
		} finally {
			resultSet.close();
		}
		return configs;
	}

	private ResultSet getTables(TableConfig table) throws SQLException {
		String catalog = table.getCatalog();
		String schema = table.getSchema();
		String name = table.getTable();
		String[] types = getTypeNames(getType(table));
		return getMetaData().getTables(catalog, schema, name, types);
	}

	private TableType getType(TableConfig table) {
		if (table.getType() == null) {
			return TableType.TABLE;
		}
		return table.getType();
	}

	public boolean exists(TableConfig table) throws SQLException {
		ResultSet resultSet = getTables(table);
		try {
			return resultSet.next();
		} finally {
			resultSet.close();
		}
	}
}
