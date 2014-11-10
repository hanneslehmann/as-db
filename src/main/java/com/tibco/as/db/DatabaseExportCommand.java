package com.tibco.as.db;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.cli.ExportCommand;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class DatabaseExportCommand extends ExportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;
	@Parameter(names = { "-table" }, description = "Table name")
	private String tableName;

	@Override
	protected void configure(IDestination destination) {
		TableDestination tableDestination = (TableDestination) destination;
		Table table = tableDestination.getTable();
		if (table.getCatalog() == null) {
			table.setCatalog(catalog);
		}
		if (table.getSchema() == null) {
			table.setSchema(schema);
		}
		if (table.getName() == null) {
			table.setName(tableName);
		}
		super.configure(destination);
	}

}
