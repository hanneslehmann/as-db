package com.tibco.as.db.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.db.Table;
import com.tibco.as.db.TableDestination;

public class DatabaseExportParameters extends DatabaseParameters {

	@Parameter(names = { "-table" }, description = "Table name")
	private String tableName;
	@Parameter(names = { "-insert_sql" }, description = "Insert SQL statement")
	private String insertSQL;

	@Override
	public void configure(TableDestination destination) {
		Table table = destination.getTable();
		if (tableName != null) {
			table.setName(tableName);
		}
		if (insertSQL != null) {
			table.setInsertSQL(insertSQL);
		}
		super.configure(destination);
	}
}
