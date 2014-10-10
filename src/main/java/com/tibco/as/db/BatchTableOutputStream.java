package com.tibco.as.db;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BatchTableOutputStream extends TableOutputStream {

	private int batchSize;
	private int count;

	public BatchTableOutputStream(PreparedStatement statement,
			IPreparedStatementAccessor[] accessors, int batchSize) {
		super(statement, accessors);
		this.batchSize = batchSize;
	}

	@Override
	public void open() throws Exception {
		count = 0;
		super.open();
	}

	@Override
	protected void close(PreparedStatement statement) throws SQLException {
		if (count > 0) {
			executeBatch(statement);
		}
		super.close(statement);
	}

	@Override
	protected void execute(PreparedStatement statement) throws SQLException {
		statement.addBatch();
		count++;
		if (count >= batchSize) {
			executeBatch(statement);
			count = 0;
		}
	}

	private void executeBatch(PreparedStatement statement) throws SQLException {
		try {
			statement.executeBatch();
		} catch (BatchUpdateException e) {
			throw e.getNextException();
		} catch (SQLException e) {
			throw e;
		}
	}

}
