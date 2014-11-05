package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.AbstractOutputStream;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.log.LogFactory;

public class TableOutputStream extends AbstractOutputStream<Object[]> implements
		IOutputStream {

	private Logger log = LogFactory.getLog(TableOutputStream.class);
	private TableDestination destination;
	private PreparedStatement statement;
	private IPreparedStatementAccessor[] accessors;

	public TableOutputStream(TableDestination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	public void open() throws Exception {
		Collection<TableDestination> tables = destination.getChannel()
				.getTables(destination);
		if (tables.isEmpty()) {
			// create table
			destination.execute(destination.getCreateSQL());
		} else {
			for (TableDestination table : tables) {
				table.copyTo(destination);
			}
		}
		String sql = destination.getInsertSQL();
		log.log(Level.FINE, "Preparing statement: {0}", sql);
		statement = destination.prepareStatement(sql);
		accessors = destination.getAccessors();
		super.open();
	}

	@Override
	public void close() throws Exception {
		super.close();
		close(statement);
	}

	protected void close(PreparedStatement statement) throws SQLException {
		statement.close();
	}

	protected void execute(PreparedStatement statement) throws SQLException {
		statement.execute();
	}

	@Override
	protected void doWrite(Object[] array) throws Exception {
		set(array);
		execute(statement);
	}

	private void set(Object[] element) throws SQLException {
		for (int index = 0; index < element.length; index++) {
			accessors[index].set(statement, element[index]);
		}
	}

	@Override
	protected Object[] newObject(int length) {
		return new Object[length];
	}

}
