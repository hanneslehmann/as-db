package com.tibco.as.db.accessors;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.tibco.as.db.IColumnAccessor;

public class ClobAccessor implements IColumnAccessor {

	private int index;

	public ClobAccessor(int index) {
		this.index = index;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		Clob clob = statement.getConnection().createClob();
		clob.setString(1, (String) value);
		statement.setClob(index, clob);
	}

	@Override
	public String get(ResultSet resultSet) throws SQLException {
		Clob blob = resultSet.getClob(index);
		if (blob == null) {
			return null;
		}
		return blob.getSubString(1, (int) blob.length());
	}

}
