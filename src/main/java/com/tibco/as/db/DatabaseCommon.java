package com.tibco.as.db;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tibco.as.db.accessors.BlobAccessor;
import com.tibco.as.db.accessors.DefaultAccessor;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.SpaceDef;

public class DatabaseCommon {

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

	public static String getFullTableName(Table table) {
		String namespace = "";
		if (table.getCatalog() != null) {
			namespace += quote(table.getCatalog()) + ".";
		}
		if (table.getSchema() != null) {
			namespace += quote(table.getSchema()) + ".";
		}
		return namespace + quote(table.getName());
	}

	private static String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public static String[] getColumnNames(Table table) {
		List<Column> columns = table.getColumns();
		String[] columnNames = new String[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			columnNames[index] = getColumnName(columns.get(index));
		}
		return columnNames;
	}

	public static String getColumnName(Column column) {
		if (column.getName() == null) {
			return quote(column.getField());
		}
		return quote(column.getName());
	}

	public static String getCommaSeparated(String[] elements) {
		String result = "";
		for (int index = 0; index < elements.length; index++) {
			if (index > 0) {
				result += ", ";
			}
			result += elements[index];
		}
		return result;
	}

	private static Collection<Column> getColumns(Connection connection,
			String catalog, String schema, String table, Column inputColumn)
			throws SQLException {
		Map<String, Short> keys = new HashMap<String, Short>();
		ResultSet keyRS = connection.getMetaData().getPrimaryKeys(catalog,
				schema, table);
		try {
			while (keyRS.next()) {
				String key = keyRS.getString(COLUMN_NAME);
				short sequence = keyRS.getShort(KEY_SEQ);
				keys.put(key, sequence);
			}
		} finally {
			keyRS.close();
		}
		Column[] columns = new Column[0];
		ResultSet columnRS = connection.getMetaData().getColumns(catalog,
				schema, table, inputColumn.getName());
		try {
			while (columnRS.next()) {
				Column column = new Column();
				column.setDecimalDigits(columnRS.getInt(DECIMAL_DIGITS));
				column.setField(inputColumn.getField());
				column.setKeySequence(keys.get(columnRS.getString(COLUMN_NAME)));
				column.setName(columnRS.getString(COLUMN_NAME));
				column.setNullable(columnRS.getBoolean(NULLABLE));
				column.setRadix(columnRS.getInt(NUM_PREC_RADIX));
				column.setSize(columnRS.getInt(COLUMN_SIZE));
				column.setType(JDBCType.valueOf(columnRS.getInt(DATA_TYPE)));
				int position = columnRS.getInt(ORDINAL_POSITION);
				if (columns.length < position) {
					columns = Arrays.copyOf(columns, position);
				}
				columns[position - 1] = column;
			}
		} finally {
			columnRS.close();
		}
		return new ArrayList<Column>(Arrays.asList(columns));
	}

	public static Collection<String> getSchemas(Connection connection,
			String catalog, String schema) throws SQLException {
		Collection<String> schemas = new ArrayList<String>();
		ResultSet rs = connection.getMetaData().getSchemas(catalog, schema);
		try {
			while (rs.next()) {
				schemas.add(rs.getString(TABLE_SCHEM));
			}
		} finally {
			rs.close();
		}
		return schemas;
	}

	public static Collection<String> getCatalogs(Connection connection)
			throws SQLException {
		Collection<String> catalogs = new ArrayList<String>();
		ResultSet rs = connection.getMetaData().getCatalogs();
		try {
			while (rs.next()) {
				catalogs.add(rs.getString(TABLE_CAT));
			}
		} finally {
			rs.close();
		}
		return catalogs;
	}

	public static Collection<Table> getTables(Connection connection, Table table)
			throws SQLException {
		Collection<Table> tables = getTables(connection, table.getCatalog(),
				table.getSchema(), table.getName(), getTypes(table));
		for (Table t : tables) {
			t.setBatchSize(table.getBatchSize());
			t.setDistributionRole(table.getDistributionRole());
			t.setFetchSize(table.getFetchSize());
			t.setSpace(table.getSpace());
			t.setSql(table.getSql());
			for (Column column : getColumns(table.getColumns())) {
				t.getColumns().addAll(
						DatabaseCommon.getColumns(connection, t.getCatalog(),
								t.getSchema(), t.getName(), column));
			}
		}
		return tables;
	}

	public static String[] getTypes(Table table) {
		return new String[] { table.getType().name() };
	}

	public static Collection<Table> getTables(Connection connection,
			String catalog, String schema, String name, String[] types)
			throws SQLException {
		Collection<Table> tables = new ArrayList<Table>();
		ResultSet rs = connection.getMetaData().getTables(catalog, schema,
				name, types);
		try {
			while (rs.next()) {
				Table table = new Table();
				table.setCatalog(rs.getString(TABLE_CAT));
				String tableName = rs.getString(TABLE_NAME);
				table.setName(tableName);
				table.setSchema(rs.getString(TABLE_SCHEM));
				table.setType(TableType.valueOf(rs.getString(TABLE_TYPE)));
				tables.add(table);
			}
		} finally {
			rs.close();
		}
		return tables;
	}

	private static Collection<Column> getColumns(Collection<Column> columns) {
		if (columns.isEmpty()) {
			return Arrays.asList(new Column());
		}
		return columns;
	}

	public static Class<?> getType(Column column) {
		switch (column.getType().getType()) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return String.class;
		case Types.NUMERIC:
		case Types.DECIMAL:
			return BigDecimal.class;
		case Types.BIT:
		case Types.BOOLEAN:
			return Boolean.class;
		case Types.TINYINT:
		case Types.SMALLINT:
			return Short.class;
		case Types.INTEGER:
			return Integer.class;
		case Types.BIGINT:
			return Long.class;
		case Types.REAL:
			return Float.class;
		case Types.FLOAT:
		case Types.DOUBLE:
			return Double.class;
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return byte[].class;
		case Types.DATE:
			return Date.class;
		case Types.TIME:
			return Time.class;
		case Types.TIMESTAMP:
			return Timestamp.class;
		case Types.CLOB:
			return Clob.class;
		case Types.BLOB:
			return byte[].class;
		default:
			return Object.class;
		}
	}

	@SuppressWarnings("unchecked")
	public static Connection getConnection(Database database)
			throws ClassNotFoundException, SQLException,
			InstantiationException, IllegalAccessException,
			MalformedURLException {
		Connection connection;
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
		return connection;
	}

	public static Map<Integer, String> getTypeInfo(Connection connection)
			throws SQLException {
		Map<Integer, String> types = new HashMap<Integer, String>();
		ResultSet typeRS = connection.getMetaData().getTypeInfo();
		while (typeRS.next()) {
			String typeName = typeRS.getString(TYPE_NAME);
			Integer dataType = typeRS.getInt(DATA_TYPE);
			types.put(dataType, typeName);
		}
		return types;
	}

	public static String getFieldName(Column column) {
		if (column.getField() == null) {
			return column.getName();
		}
		return column.getField();
	}

	public static FieldDef getFieldDef(SpaceDef spaceDef, Column column) {
		String fieldName = DatabaseCommon.getFieldName(column);
		for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
			if (fieldDef.getName().equalsIgnoreCase(fieldName)) {
				return fieldDef;
			}
		}
		return null;
	}

	public static IPreparedStatementAccessor[] getAccessors(Table table,
			PreparedStatement statement) {
		IPreparedStatementAccessor[] accessors = new IPreparedStatementAccessor[table
				.getColumns().size()];
		for (int index = 0; index < accessors.length; index++) {
			Column column = table.getColumns().get(index);
			accessors[index] = getAccessor(statement, index + 1, column
					.getType().getType());
		}
		return accessors;
	}

	private static IPreparedStatementAccessor getAccessor(
			PreparedStatement statement, int index, int type) {
		switch (type) {
		case Types.BLOB:
			return new BlobAccessor(index);
		default:
			return new DefaultAccessor(index, type);
		}
	}

}
