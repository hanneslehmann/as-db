package com.tibco.as.db.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.db.Table;
import com.tibco.as.db.TableDestination;

public class DatabaseParameters {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;

	public void configure(TableDestination destination) {
		Table table = destination.getTable();
		if (catalog != null) {
			table.setCatalog(catalog);
		}
		if (schema != null) {
			table.setSchema(schema);
		}
	}

	public String getCatalog() {
		return catalog;
	}

	public String getSchema() {
		return schema;
	}
}
