package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.cli.AbstractCommandExport;
import com.tibco.as.space.Metaspace;

@Parameters(commandNames = "export", commandDescription = "Export tables")
public class ExportCommand extends AbstractCommandExport {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;

	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;

	private Application application;

	public ExportCommand(Application application) {
		this.application = application;
	}

	@Override
	protected Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace, Collection<String> spaceNames) {
		Collection<IMetaspaceTransfer> transfers = new ArrayList<IMetaspaceTransfer>();
		Database database = application.getDatabase();
		for (String spaceName : spaceNames) {
			Table table = new Table();
			table.setCatalog(catalog);
			table.setSchema(schema);
			table.setSpace(spaceName);
			database.getTables().add(table);
		}
		Exporter exporter = new Exporter(metaspace, database);
		Export transfer = new Export();
		configure(transfer);
		exporter.setDefaultTransfer(transfer);
		transfers.add(exporter);
		return transfers;
	}

}
