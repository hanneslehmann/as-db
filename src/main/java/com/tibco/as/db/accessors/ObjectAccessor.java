package com.tibco.as.db.accessors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.tibco.as.db.IColumnAccessor;

public class ObjectAccessor implements IColumnAccessor {

	private int index;
	private int sqlType;

	public ObjectAccessor(int index, int sqlType) {
		this.index = index;
		this.sqlType = sqlType;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		statement.setObject(index, value, sqlType);
	}

	@Override
	public Object get(ResultSet resultSet) throws SQLException {
		return resultSet.getObject(index);
	}

}
