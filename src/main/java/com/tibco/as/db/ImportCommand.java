package com.tibco.as.db;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.cli.AbstractImportCommand;
import com.tibco.as.space.Metaspace;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class ImportCommand extends AbstractImportCommand {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;

	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;

	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@Parameter(names = { "-fetch_size" }, description = "Fetch size")
	private Integer fetchSize;

	@Parameter(names = { "-sql" }, description = "SQL query")
	private String sql;

	@Parameter(names = { "-type" }, description = "Table type", converter = TableTypeConverter.class, validateWith = TableTypeConverter.class)
	private TableType tableType;

	private Application application;

	public ImportCommand(Application application) {
		this.application = application;
	}

	@Override
	protected Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace) throws FileNotFoundException {
		Collection<IMetaspaceTransfer> transfers = new ArrayList<IMetaspaceTransfer>();
		Database database = application.getDatabase();
		for (String tableName : tableNames) {
			Table table = new Table();
			table.setCatalog(catalog);
			table.setSchema(schema);
			table.setName(tableName);
			table.setFetchSize(fetchSize);
			table.setSql(sql);
			table.setType(tableType);
			database.getTables().add(table);
		}
		DatabaseImporter importer = new DatabaseImporter(metaspace, database);
		DatabaseImport transfer = new DatabaseImport();
		configure(transfer);
		importer.setDefaultTransfer(transfer);
		transfers.add(importer);
		return transfers;
	}

}
