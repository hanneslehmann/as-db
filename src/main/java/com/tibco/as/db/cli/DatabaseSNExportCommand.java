package com.tibco.as.db.cli;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.db.DatabaseChannel;
import com.tibco.as.db.TableDestination;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.cli.AbstractSNExportCommand;

@Parameters(commandNames = "export-sn", commandDescription = "Export shared-nothing persistence files to a database")
public class DatabaseSNExportCommand extends AbstractSNExportCommand {

	@ParametersDelegate
	private DatabaseExportParameters parameters = new DatabaseExportParameters();

	@Override
	protected ITransfer getTransfer(IDestination destination) {
		parameters.configure((TableDestination) destination);
		return super.getTransfer(destination);
	}

	@Override
	protected IDestination newDestination(IChannel channel) {
		return new TableDestination((DatabaseChannel) channel);
	}

}
