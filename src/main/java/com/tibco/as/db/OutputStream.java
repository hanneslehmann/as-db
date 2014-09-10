package com.tibco.as.db;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IOutputStream;

public class OutputStream implements IOutputStream<Object[]> {

	private PreparedStatement statement;
	private IPreparedStatementAccessor[] accessors;

	public OutputStream(PreparedStatement statement,
			IPreparedStatementAccessor[] accessors) {
		this.statement = statement;
		this.accessors = accessors;
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
		try {
			statement.executeBatch();
		} catch (BatchUpdateException e) {
			throw e.getNextException();
		}
	}

	@Override
	public void write(Object[] element) throws Exception {
		set(element);
		statement.execute();
	}

	private void set(Object[] element) throws SQLException {
		for (int index = 0; index < element.length; index++) {
			accessors[index].set(statement, element[index]);
		}
	}
}
