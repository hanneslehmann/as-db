package com.tibco.as.db;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.Destination;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.ExportCommand;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class DatabaseExportCommand extends ExportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;
	@Parameter(names = { "-table" }, description = "Table name")
	private String tableName;
	@Parameter(names = { "-insert_sql" }, description = "Insert SQL statement")
	private String insertSQL;

	@Override
	protected Destination createDestination(IChannel channel) {
		Table table = new Table();
		table.setCatalog(catalog);
		table.setSchema(schema);
		table.setName(tableName);
		if (insertSQL != null) {
			table.setInsertSQL(insertSQL);
		}
		return new TableDestination((DatabaseChannel) channel, table);
	}

}
