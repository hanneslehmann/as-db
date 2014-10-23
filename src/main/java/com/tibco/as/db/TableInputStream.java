package com.tibco.as.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tibco.as.io.IInputStream;

public class TableInputStream extends TableStream implements IInputStream {

	private DatabaseChannel channel;
	private TableConfig config;
	private ResultSet resultSet;
	private IPreparedStatementAccessor[] accessors;
	private Long position;
	private Long count;

	public TableInputStream(DatabaseChannel channel, TableConfig config) {
		super(config);
		this.channel = channel;
		this.config = config;
	}

	@Override
	public void open() throws SQLException {
		resultSet = channel.executeQuery(config.getSelectSQL());
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
		accessors = getAccessors();
		count = getCount();
		position = 0L;
	}

	private Long getCount() throws SQLException {
		if (config.getCountSQL() == null) {
			return null;
		}
		ResultSet resultSet = channel.executeQuery(config.getCountSQL());
		try {
			if (resultSet.next()) {
				return resultSet.getLong(1);
			}
			return null;
		} finally {
			resultSet.close();
		}
	}

	@Override
	public Long size() {
		return count;
	}

	@Override
	public Object[] read() throws SQLException {
		if (resultSet.next()) {
			Object[] result = new Object[accessors.length];
			for (int index = 0; index < accessors.length; index++) {
				result[index] = accessors[index].get(resultSet);
			}
			position++;
			return result;
		}
		return null;
	}

	@Override
	public Long getPosition() {
		return position;
	}

	@Override
	public void close() throws SQLException {
		if (resultSet.isClosed()) {
			return;
		}
		resultSet.close();
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}