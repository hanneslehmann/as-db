package com.tibco.as.db.accessors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface IColumnAccessor {

	void set(PreparedStatement statement, Object value) throws SQLException;

	Object get(ResultSet resultSet) throws SQLException;

}
