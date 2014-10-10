package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DefaultAccessor implements IPreparedStatementAccessor {

	private int index;
	private int sqlType;

	public DefaultAccessor(int index, int sqlType) {
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
