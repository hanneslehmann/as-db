package com.tibco.as.db;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

	private static final char QUOTE = '\"';

	public static String getFullTableName(Table table) {
		String namespace = "";
		if (table.getCatalog() != null) {
			namespace += table.getCatalog() + ".";
		}
		if (table.getSchema() != null) {
			namespace += table.getSchema() + ".";
		}
		return namespace + table.getName();
	}

	private static String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	public static String[] getColumnNames(Table table) {
		List<Column> columns = table.getColumns();
		String[] columnNames = new String[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			columnNames[index] = columns.get(index).getName();
		}
		return columnNames;
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
			Table table, Column inputColumn) throws SQLException {
		Map<String, Short> keys = new HashMap<String, Short>();
		ResultSet keyRS = connection.getMetaData().getPrimaryKeys(
				table.getCatalog(), table.getSchema(), table.getName());
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
		ResultSet columnRS = connection.getMetaData().getColumns(
				table.getCatalog(), table.getSchema(), table.getName(),
				inputColumn.getName());
		try {
			while (columnRS.next()) {
				String columnName = columnRS.getString(COLUMN_NAME);
				Column column = new Column();
				column.setDecimalDigits(columnRS.getInt(DECIMAL_DIGITS));
				String fieldName = inputColumn.getField();
				if (fieldName == null) {
					fieldName = columnName;
				}
				column.setField(fieldName);
				column.setKeySequence(keys.get(columnName));
				column.setName(columnName);
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
			String catalog) throws SQLException {
		Collection<String> schemas = new ArrayList<String>();
		ResultSet rs = connection.getMetaData().getSchemas(catalog, null);
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

	public static Collection<Table> getTables(Connection connection,
			Table inputTable) throws SQLException {
		Collection<Table> tables = new ArrayList<Table>();
		ResultSet rs = connection.getMetaData().getTables(
				inputTable.getCatalog(), inputTable.getSchema(),
				inputTable.getName(),
				new String[] { inputTable.getType().name() });
		try {
			while (rs.next()) {
				Table table = new Table();
				table.setBatchSize(inputTable.getBatchSize());
				table.setCatalog(rs.getString(TABLE_CAT));
				table.setDistributionRole(inputTable.getDistributionRole());
				table.setFetchSize(inputTable.getFetchSize());
				String tableName = rs.getString(TABLE_NAME);
				table.setName(tableName);
				table.setSchema(rs.getString(TABLE_SCHEM));
				String spaceName = inputTable.getSpace();
				if (spaceName == null) {
					spaceName = tableName;
				}
				table.setSpace(spaceName);
				table.setSql(inputTable.getSql());
				table.setType(TableType.valueOf(rs.getString(TABLE_TYPE)));
				for (Column column : getColumns(inputTable.getColumns())) {
					table.getColumns().addAll(
							DatabaseCommon
									.getColumns(connection, table, column));
				}
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
			return Blob.class;
		default:
			return Object.class;
		}
	}

	public static Map<String, Table> getTableMap(Connection connection,
			Table inputTable) throws SQLException {
		Map<String, Table> map = new LinkedHashMap<String, Table>();
		for (Table table : getTables(connection, inputTable)) {
			map.put(getFullTableName(table), table);
		}
		return map;
	}

	public static Connection getConnection(Database database)
			throws ClassNotFoundException, SQLException {
		Class.forName(database.getDriver());
		Connection connection = DriverManager.getConnection(database.getUrl(),
				database.getUser(), database.getPassword());
		connection.setAutoCommit(true);
		return connection;
	}
}
