package com.tibco.as.db.accessors;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.tibco.as.db.IPreparedStatementAccessor;

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

}
