package com.tibco.as.db.cli;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.db.DatabaseChannel;
import com.tibco.as.db.Table;
import com.tibco.as.db.TableDestination;
import com.tibco.as.db.TableType;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IExecutor;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.cli.ImportCommand;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class DatabaseImportCommand extends ImportCommand {

	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@ParametersDelegate
	private DatabaseImportParameters parameters = new DatabaseImportParameters();

	@Override
	protected ITransfer getTransfer(IDestination destination) {
		parameters.configure((TableDestination) destination);
		return super.getTransfer(destination);
	}

	@Override
	public IExecutor getExecutor(IChannel channel) throws Exception {
		DatabaseChannel dbChannel = (DatabaseChannel) channel;
		if (dbChannel.getDestinations().isEmpty()) {
			if (tableNames.isEmpty()) {
				tableNames.add("%");
			}
		}
		String catalog = parameters.getCatalog();
		String schema = parameters.getSchema();
		TableType type = parameters.getType();
		for (String name : tableNames) {
			for (Table table : dbChannel.getTables(catalog, schema, name, type)) {
				dbChannel.addDestination().setTable(table);
			}
		}
		return super.getExecutor(channel);
	}

}
