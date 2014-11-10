package com.tibco.as.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tibco.as.io.IInputStream;

public class TableInputStream implements IInputStream {

	private TableDestination destination;
	private ResultSet resultSet;
	private IPreparedStatementAccessor[] accessors;
	private Long position;
	private Long count;

	public TableInputStream(TableDestination destination) {
		this.destination = destination;
	}

	@Override
	public synchronized void open() throws SQLException {
		resultSet = destination.executeQuery(destination.getSelectSQL());
		if (destination.getTable().getFetchSize() != null) {
			resultSet.setFetchSize(destination.getTable().getFetchSize());
		}
		ResultSetMetaData metaData = resultSet.getMetaData();
		for (int index = 1; index <= metaData.getColumnCount(); index++) {
			String columnName = metaData.getColumnName(index);
			int precision = metaData.getPrecision(index);
			int scale = metaData.getScale(index);
			boolean nullable = metaData.isNullable(index) == ResultSetMetaData.columnNullable;
			int dataType = metaData.getColumnType(index);
			Column column = destination.getColumn(columnName);
			if (column == null) {
				column = new Column();
				column.setName(columnName);
				destination.getTable().getColumns().add(column);
			}
			column.setSize(precision);
			column.setDecimals(scale);
			column.setNullable(nullable);
			column.setType(JDBCType.valueOf(dataType));
		}
		accessors = destination.getAccessors();
		count = getCount();
		position = 0L;
	}

	private Long getCount() throws SQLException {
		if (destination.getCountSQL() == null) {
			return null;
		}
		String sql = destination.getCountSQL();
		ResultSet resultSet = destination.executeQuery(sql);
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
	public synchronized void close() throws SQLException {
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