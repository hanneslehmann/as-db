package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.db.accessors.IColumnAccessor;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.util.log.LogFactory;

public class TableOutputStream implements IOutputStream<Object[]> {

	private Logger log = LogFactory.getLog(TableOutputStream.class);
	private TableDestination destination;
	private PreparedStatement statement;
	private IColumnAccessor[] accessors;

	public TableOutputStream(TableDestination destination) {
		this.destination = destination;
	}

	@Override
	public synchronized void open() throws Exception {
		Table table = destination.getTable();
		DatabaseChannel channel = destination.getChannel();
		Collection<Table> tables = channel.getTables(table.getCatalog(),
				table.getSchema(), table.getName(), table.getType());
		if (tables.isEmpty()) {
			// create table
			destination.execute(destination.getCreateSQL());
		} else {
			destination.setColumnsFromMetaData();
		}
		String sql = destination.getInsertSQL();
		log.log(Level.FINE, "Preparing statement: {0}", sql);
		statement = destination.prepareStatement(sql);
		accessors = destination.getColumnAccessors();
	}

	@Override
	public synchronized void close() throws Exception {
		close(statement);
	}

	@Override
	public void write(Object[] array) throws Exception {
		set(array);
		statement.execute();
	}

	private void set(Object[] array) throws SQLException {
		for (int index = 0; index < accessors.length; index++) {
			accessors[index].set(statement, array[index]);
		}
	}

	protected void close(PreparedStatement statement) throws SQLException {
		statement.close();
	}

	@Override
	public void write(List<Object[]> arrays) throws Exception {
		for (Object[] array : arrays) {
			set(array);
			statement.addBatch();
		}
		statement.executeBatch();
	}

	@Override
	public Object[] newObject() {
		return new Object[accessors.length];
	}
}
