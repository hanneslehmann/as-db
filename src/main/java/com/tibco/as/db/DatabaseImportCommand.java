package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.ChannelTransfer;
import com.tibco.as.io.Destination;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.ImportCommand;
import com.tibco.as.util.log.LogFactory;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class DatabaseImportCommand extends ImportCommand {

	private Logger log = LogFactory.getLog(DatabaseImportCommand.class);

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
	private TableType type;
	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@Override
	protected Destination createDestination(IChannel channel) {
		Table table = new Table();
		table.setCatalog(catalog);
		table.setSchema(schema);
		table.setFetchSize(fetchSize);
		table.setSelectSQL(selectSQL);
		table.setCountSQL(countSQL);
		table.setType(type);
		return new TableDestination((DatabaseChannel) channel, table);
	}

	@Override
	public ChannelTransfer getTransfer(IChannel channel) throws Exception {
		DatabaseChannel dbChannel = (DatabaseChannel) channel;
		if (dbChannel.getDestinations().isEmpty()) {
			if (tableNames.isEmpty()) {
				log.warning("No table specified");
			}
		}
		for (String name : tableNames) {
			Collection<Table> tables = dbChannel.getTables(catalog, schema,
					name, type);
			for (Table table : tables) {
				TableDestination destination = new TableDestination(dbChannel,
						table);
				destination.setTable(table);
				channel.getDestinations().add(destination);
			}
		}
		return super.getTransfer(channel);
	}
}
