package com.tibco.as.db.accessors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;

public class TimeAccessor implements IColumnAccessor {

	private int index;

	public TimeAccessor(int index) {
		this.index = index;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		statement.setTime(index, (Time) value);
	}

	@Override
	public Time get(ResultSet resultSet) throws SQLException {
		return resultSet.getTime(index);
	}

}
