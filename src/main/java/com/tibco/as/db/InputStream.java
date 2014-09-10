package com.tibco.as.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IInputStream;

public class InputStream implements IInputStream<Object[]> {

	private ResultSet resultSet;
	private IPreparedStatementAccessor[] accessors;
	private long position;

	public InputStream(ResultSet resultSet,
			IPreparedStatementAccessor[] accessors) {
		this.resultSet = resultSet;
		this.accessors = accessors;
	}

	@Override
	public void open() throws SQLException {
	}

	@Override
	public long size() {
		return IInputStream.UNKNOWN_SIZE;
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
	public long getPosition() {
		return position;
	}

	@Override
	public void close() throws SQLException {
		if (resultSet == null) {
			return;
		}
		Statement statement = resultSet.getStatement();
		resultSet.close();
		statement.close();
	}

	@Override
	public boolean isClosed() {
		try {
			return resultSet == null || resultSet.isClosed();
		} catch (SQLException e) {
			EventManager.error(e, "Could not get state of result set");
			return false;
		}
	}

	@Override
	public String getName() {
		try {
			return MessageFormat.format("table ''{0}''", resultSet
					.getMetaData().getTableName(1));
		} catch (SQLException e) {
			EventManager.error(e, "Could not get result set's table name");
			return "Unkwown table";
		}
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}