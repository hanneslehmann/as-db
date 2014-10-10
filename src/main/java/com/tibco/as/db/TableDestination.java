package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tibco.as.io.AbstractDestination;
import com.tibco.as.io.FieldConfig;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;

public class TableDestination extends AbstractDestination {

	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
	private static final String NULLABLE = "NULLABLE";
	private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
	private static final String KEY_SEQ = "KEY_SEQ";
	private static final char QUOTE = '\"';
	private static final String SELECT = "SELECT {0} FROM {1}";

	private DatabaseChannel channel;
	private TableConfig config;

	public TableDestination(DatabaseChannel channel, TableConfig config) {
		super(channel, config);
		this.channel = channel;
		this.config = config;
	}

	@Override
	protected IOutputStream getOutputStream() throws SQLException {
		ResultSet resultSet = channel.getTables(config);
		try {
			if (resultSet.next()) { // table already exists, populate config
				List<FieldConfig> fields = new ArrayList<FieldConfig>();
				for (FieldConfig field : getTableColumns()) {
					ColumnConfig column = (ColumnConfig) field;
					ResultSet columnRS = channel.getMetaData().getColumns(
							config.getCatalog(), config.getSchema(),
							config.getTable(), column.getColumnName());
					Map<Integer, ColumnConfig> columns = new TreeMap<Integer, ColumnConfig>();
					try {
						while (columnRS.next()) {
							ColumnConfig found = column.clone();
							found.setDecimalDigits(columnRS
									.getInt(DECIMAL_DIGITS));
							found.setColumnName(columnRS.getString(COLUMN_NAME));
							found.setColumnNullable(columnRS
									.getBoolean(NULLABLE));
							found.setRadix(columnRS.getInt(NUM_PREC_RADIX));
							found.setColumnSize(columnRS.getInt(COLUMN_SIZE));
							int dataType = columnRS.getInt(DATA_TYPE);
							found.setColumnType(JDBCType.valueOf(dataType));
							columns.put(columnRS.getInt(ORDINAL_POSITION),
									found);
						}
					} finally {
						columnRS.close();
					}
					fields.addAll(columns.values());
				}
				config.setFields(fields);
				setPrimaryKeys();
			} else {
				String sql = getCreateSQL();
				channel.execute(sql);
			}
		} finally {
			resultSet.close();
		}
		if (config.getInsertSQL() == null) {
			String tableName = getFullyQualifiedName();
			String[] columnNames = getColumnNames();
			String[] questionMarks = new String[columnNames.length];
			Arrays.fill(questionMarks, "?");
			String sql = MessageFormat.format(
					"INSERT INTO {0} ({1}) VALUES ({2})", tableName,
					getCommaSeparated(columnNames),
					getCommaSeparated(questionMarks));
			config.setInsertSQL(sql);
		}
		String sql = config.getInsertSQL();
		PreparedStatement statement = channel.prepareStatement(sql);
		IPreparedStatementAccessor[] accessors = getAccessors();
		int batchSize = config.getTableBatchSize();
		if (batchSize > 1) {
			return new BatchTableOutputStream(statement, accessors, batchSize);
		}
		return new TableOutputStream(statement, accessors);
	}

	@Override
	protected TableInputStream getInputStream() throws Exception {
		if (config.getSelectSQL() == null) {
			config.setSelectSQL(MessageFormat.format(SELECT,
					getCommaSeparated(getColumnNames()),
					getFullyQualifiedName()));
			if (config.getCountSQL() == null) {
				config.setCountSQL(MessageFormat.format(SELECT, "COUNT(*)",
						getFullyQualifiedName()));
			}
		}
		ResultSet resultSet = channel.executeQuery(config.getSelectSQL());
		if (config.getFetchSize() != null) {
			resultSet.setFetchSize(config.getFetchSize());
		}
		ResultSetMetaData metaData = resultSet.getMetaData();
		for (int index = 1; index <= metaData.getColumnCount(); index++) {
			String columnName = metaData.getColumnName(index);
			int precision = metaData.getPrecision(index);
			int scale = metaData.getScale(index);
			boolean nullable = metaData.isNullable(index) == ResultSetMetaData.columnNullable;
			int dataType = metaData.getColumnType(index);
			ColumnConfig column = config.getColumn(columnName);
			column.setColumnSize(precision);
			column.setDecimalDigits(scale);
			column.setColumnNullable(nullable);
			column.setColumnType(JDBCType.valueOf(dataType));
		}
		setPrimaryKeys();
		return new TableInputStream(resultSet, getAccessors(), getCount());
	}

	private IPreparedStatementAccessor[] getAccessors() {
		Collection<IPreparedStatementAccessor> result = new ArrayList<IPreparedStatementAccessor>();
		int columnIndex = 1;
		for (FieldConfig field : config.getFields()) {
			ColumnConfig column = (ColumnConfig) field;
			result.add(getAccessor(columnIndex, column.getColumnType()));
			columnIndex++;
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

	private long getCount() throws SQLException {
		if (config.getCountSQL() == null) {
			return IInputStream.UNKNOWN_SIZE;
		}
		ResultSet resultSet = channel.executeQuery(config.getCountSQL());
		try {
			if (resultSet.next()) {
				return resultSet.getLong(1);
			}
			return IInputStream.UNKNOWN_SIZE;
		} finally {
			resultSet.close();
		}
	}

	@Override
	protected String getExportName() {
		return config.getTable();
	}

	@Override
	protected String getImportName() {
		return config.getTable();
	}

	private String getFullyQualifiedName() {
		String namespace = "";
		if (config.getCatalog() != null) {
			namespace += quote(config.getCatalog()) + ".";
		}
		if (config.getSchema() != null) {
			namespace += quote(config.getSchema()) + ".";
		}
		return namespace + quote(config.getTable());
	}

	private String quote(String name) {
		return QUOTE + name + QUOTE;
	}

	private String[] getColumnNames() {
		List<FieldConfig> fields = config.getFields();
		if (fields.isEmpty()) {
			return new String[] { "*" };
		}
		Collection<String> columnNames = new ArrayList<String>();
		for (FieldConfig field : fields) {
			ColumnConfig column = (ColumnConfig) field;
			columnNames.add(quote(column.getColumnName()));
		}
		return columnNames.toArray(new String[columnNames.size()]);
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

	private String getCreateSQL() throws SQLException {
		String query = "";
		query += "CREATE TABLE " + getFullyQualifiedName() + " (";
		for (FieldConfig field : config.getFields()) {
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
		for (String key : config.getPrimaryKeys()) {
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

	private void setPrimaryKeys() throws SQLException {
		ResultSet keyRS = channel.getMetaData().getPrimaryKeys(
				config.getCatalog(), config.getSchema(), config.getTable());
		try {
			while (keyRS.next()) {
				ColumnConfig column = config.getColumn(keyRS
						.getString(COLUMN_NAME));
				column.setKeySequence(keyRS.getShort(KEY_SEQ));
			}
		} finally {
			keyRS.close();
		}
	}

	private List<FieldConfig> getTableColumns() {
		if (config.getFields().isEmpty()) {
			return Arrays.asList((FieldConfig) new ColumnConfig());
		}
		return config.getFields();
	}

}
