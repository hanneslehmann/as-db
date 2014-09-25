package com.tibco.as.db;

import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.IDestination;
import com.tibco.as.space.Metaspace;

public class DatabaseChannel extends AbstractChannel {

	private Database database;

	private DatabaseConnection connection;

	private boolean keepOpen;

	public DatabaseChannel(Metaspace metaspace, Database database) {
		super(metaspace);
		this.database = database;
	}

	public Database getDatabase() {
		return database;
	}

	@Override
	public void open() throws Exception {
		connection = new DatabaseConnection(database);
		connection.open();
		super.open();
	}

	@Override
	protected IDestination getDestination(DestinationConfig config)
			throws Exception {
		return new TableDestination(this, (TableConfig) config, connection);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (keepOpen) {
			return;
		}
		if (connection == null) {
			return;
		}
		connection.close();
		connection = null;
	}

	public DatabaseConnection getConnection() {
		return connection;
	}

	public void setKeepOpen(boolean keepOpen) {
		this.keepOpen = keepOpen;
	}
}
