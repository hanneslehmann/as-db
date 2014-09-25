package com.tibco.as.db;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.tibco.as.io.IOutputStream;

public class TableOutputStream implements IOutputStream<Object[]> {

	private PreparedStatement statement;
	private IPreparedStatementAccessor[] accessors;

	public TableOutputStream(PreparedStatement statement,
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