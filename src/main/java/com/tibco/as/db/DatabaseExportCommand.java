package com.tibco.as.db;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.cli.AbstractExportCommand;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class DatabaseExportCommand extends AbstractExportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;
	@Parameter(names = { "-table_batch_size" }, description = "Size of ")
	private Integer insertBatchSize;

	@Override
	protected void configure(DestinationConfig config) {
		TableConfig table = (TableConfig) config;
		if (table.getCatalog() == null) {
			table.setCatalog(catalog);
		}
		if (table.getSchema() == null) {
			table.setSchema(schema);
		}
		table.setTableBatchSize(insertBatchSize);
		super.configure(config);
	}

}
