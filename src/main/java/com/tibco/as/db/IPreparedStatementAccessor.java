package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface IPreparedStatementAccessor {

	void set(PreparedStatement statement, Object value) throws SQLException;

}
