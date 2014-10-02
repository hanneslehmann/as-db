package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.cli.AbstractImportCommand;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class DatabaseImportCommand extends AbstractImportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;

	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;

	@Parameter(names = { "-fetch_size" }, description = "Fetch size")
	private Integer fetchSize;

	@Parameter(names = { "-select_sql" }, description = "Select query")
	private String selectSQL;

	@Parameter(names = { "-count_sql" }, description = "Select count query")
	private String countSQL;

	@Parameter(names = { "-type" }, description = "Table type", converter = TableTypeConverter.class, validateWith = TableTypeConverter.class)
	private TableType tableType;

	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@Override
	protected void configure(DestinationConfig config) {
		TableConfig table = (TableConfig) config;
		if (table.getCatalog() == null) {
			table.setCatalog(catalog);
		}
		if (table.getSchema() == null) {
			table.setSchema(schema);
		}
		if (table.getFetchSize() == null) {
			table.setFetchSize(fetchSize);
		}
		if (table.getSelectSQL() == null) {
			table.setSelectSQL(selectSQL);
		}
		if (table.getCountSQL() == null) {
			table.setCountSQL(countSQL);
		}
		if (table.getType() == null) {
			table.setType(tableType);
		}
		super.configure(config);
	}

	@Override
	protected void configure(Collection<DestinationConfig> destinations) {
		for (String tableName : tableNames) {
			TableConfig table = new TableConfig();
			table.setTable(tableName);
			destinations.add(table);
		}
	}
}
