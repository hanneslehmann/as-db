package com.tibco.as.db;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.Destination;
import com.tibco.as.io.cli.ExportCommand;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class DatabaseExportCommand extends ExportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;
	@Parameter(names = { "-table" }, description = "Table name")
	private String tableName;
	@Parameter(names = { "-insert_batch_size" }, description = "Number of records to include in batch inserts")
	private Integer insertBatchSize;

	@Override
	protected void configure(Destination config) {
		TableDestination table = (TableDestination) config;
		if (table.getCatalog() == null) {
			table.setCatalog(catalog);
		}
		if (table.getSchema() == null) {
			table.setSchema(schema);
		}
		if (table.getTable() == null) {
			table.setTable(tableName);
		}
		table.setTableBatchSize(insertBatchSize);
		super.configure(config);
	}

}
