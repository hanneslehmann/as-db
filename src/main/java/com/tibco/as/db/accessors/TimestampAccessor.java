package com.tibco.as.db.accessors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimestampAccessor implements IColumnAccessor {

	private int index;

	public TimestampAccessor(int index) {
		this.index = index;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		statement.setTimestamp(index, (Timestamp) value);
	}

	@Override
	public Timestamp get(ResultSet resultSet) throws SQLException {
		return resultSet.getTimestamp(index);
	}

}
