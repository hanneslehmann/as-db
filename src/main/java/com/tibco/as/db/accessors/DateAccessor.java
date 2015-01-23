package com.tibco.as.db.accessors;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DateAccessor implements IColumnAccessor {

	private int index;

	public DateAccessor(int index) {
		this.index = index;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		statement.setDate(index, (Date) value);
	}

	@Override
	public Date get(ResultSet resultSet) throws SQLException {
		return resultSet.getDate(index);
	}

}
