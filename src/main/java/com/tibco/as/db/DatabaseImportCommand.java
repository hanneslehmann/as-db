package com.tibco.as.db;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.Channel;
import com.tibco.as.io.ChannelImport;
import com.tibco.as.io.Destination;
import com.tibco.as.io.cli.ImportCommand;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class DatabaseImportCommand extends ImportCommand {

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
	@Parameter(names = { "-insert_sql" }, description = "Insert SQL statement")
	private String insertSQL;
	@Parameter(names = { "-type" }, description = "Table type", converter = TableTypeConverter.class, validateWith = TableTypeConverter.class)
	private TableType tableType;
	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@Override
	protected void configure(Destination config) {
		TableDestination table = (TableDestination) config;
		if (catalog != null) {
			table.setCatalog(catalog);
		}
		if (schema != null) {
			table.setSchema(schema);
		}
		if (fetchSize != null) {
			table.setFetchSize(fetchSize);
		}
		if (selectSQL != null) {
			table.setSelectSQL(selectSQL);
		}
		if (countSQL != null) {
			table.setCountSQL(countSQL);
		}
		if (insertSQL != null) {
			table.setInsertSQL(insertSQL);
		}
		if (tableType != null) {
			table.setType(tableType);
		}
		super.configure(config);
	}

	@Override
	public ChannelImport getTransfer(Channel channel) throws Exception {
		DatabaseChannel databaseChannel = (DatabaseChannel) channel;
		for (String tableName : tableNames) {
			databaseChannel.addDestination().setTable(tableName);
		}
		return super.getTransfer(channel);
	}

}
