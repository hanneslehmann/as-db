package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IOutputStream;

public class DatabaseOutputStream implements IOutputStream<Object[]> {

	private PreparedStatement statement;

	public DatabaseOutputStream(PreparedStatement statement) {
		this.statement = statement;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public void close() throws Exception {
		statement.executeBatch();
		statement.close();
		statement = null;
	}

	@Override
	public boolean isClosed() {
		try {
			return statement == null || statement.isClosed();
		} catch (SQLException e) {
			EventManager.error(e,
					"Could not retrieve whether statement is closed");
			return false;
		}
	}

	@Override
	public void write(List<Object[]> elements) throws Exception {
		for (Object[] element : elements) {
			set(element);
			statement.addBatch();
		}
		statement.executeBatch();
	}

	@Override
	public void write(Object[] element) throws Exception {
		set(element);
		statement.execute();
	}

	private void set(Object[] element) throws SQLException {
		for (int index = 0; index < element.length; index++) {
			statement.setObject(index + 1, element[index]);
		}
	}
}
