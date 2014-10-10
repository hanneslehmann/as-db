package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.tibco.as.io.IOutputStream;

public class TableOutputStream implements IOutputStream {

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
		close(statement);
	}

	protected void close(PreparedStatement statement) throws SQLException {
		statement.close();
	}

	@Override
	public void write(Object element) throws Exception {
		set((Object[]) element);
		execute(statement);
	}

	protected void execute(PreparedStatement statement) throws SQLException {
		statement.execute();
	}

	private void set(Object[] element) throws SQLException {
		for (int index = 0; index < element.length; index++) {
			accessors[index].set(statement, element[index]);
		}
	}
}
