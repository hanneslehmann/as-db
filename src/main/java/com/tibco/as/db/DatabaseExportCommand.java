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

	@Override
	protected TableConfig createConfig() {
		return new TableConfig();
	}

	@Override
	protected void configure(DestinationConfig config) {
		TableConfig export = (TableConfig) config;
		Table table = export.getTable();
		if (table.getCatalog() == null) {
			table.setCatalog(catalog);
		}
		if (table.getSchema() == null) {
			table.setSchema(schema);
		}
		super.configure(export);
	}

}
