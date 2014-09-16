package com.tibco.as.db;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.cli.AbstractExportCommand;
import com.tibco.as.space.Metaspace;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class ExportCommand extends AbstractExportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;
	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;
	@Parameter(names = { "-keep_connection_open" }, description = "Keep database connection open after execution")
	private boolean keepConnectionOpen;

	private Application application;

	public ExportCommand(Application application) {
		this.application = application;
	}

	@Override
	protected Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace, Collection<String> spaceNames)
			throws FileNotFoundException {
		Collection<IMetaspaceTransfer> transfers = new ArrayList<IMetaspaceTransfer>();
		Database database = application.getDatabase();
		for (String spaceName : spaceNames) {
			Table table = new Table();
			table.setCatalog(catalog);
			table.setSchema(schema);
			table.setSpace(spaceName);
			database.getTables().add(table);
		}
		DatabaseExporter exporter = new DatabaseExporter(metaspace, database);
		exporter.setKeepConnectionOpen(keepConnectionOpen);
		DatabaseExport transfer = new DatabaseExport();
		configure(transfer);
		exporter.setDefaultTransfer(transfer);
		transfers.add(exporter);
		return transfers;
	}

}
