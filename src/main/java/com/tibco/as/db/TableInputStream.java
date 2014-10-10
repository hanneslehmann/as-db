package com.tibco.as.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public class TableInputStream implements IInputStream {

	private Logger log = LogFactory.getLog(TableInputStream.class);

	private ResultSet resultSet;
	private IPreparedStatementAccessor[] accessors;
	private long position;
	private long count;

	public TableInputStream(ResultSet resultSet,
			IPreparedStatementAccessor[] accessors, long count) {
		this.resultSet = resultSet;
		this.accessors = accessors;
		this.count = count;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	@Override
	public void open() throws SQLException {
	}

	@Override
	public long size() {
		return count;
	}

	@Override
	public Object[] read() throws SQLException {
		if (resultSet.next()) {
			Object[] result = new Object[accessors.length];
			for (int index = 0; index < accessors.length; index++) {
				result[index] = accessors[index].get(resultSet);
			}
			position++;
			return result;
		}
		return null;
	}

	@Override
	public long getPosition() {
		return position;
	}

	@Override
	public void close() throws SQLException {
		if (resultSet == null) {
			return;
		}
		resultSet.close();
		resultSet = null;
	}

	@Override
	public boolean isClosed() {
		try {
			return resultSet == null || resultSet.isClosed();
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Could not get state of result set", e);
			return false;
		}
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}