package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.cli.AbstractCommandImport;
import com.tibco.as.space.Metaspace;

@Parameters(commandNames = "import", commandDescription = "Import tables")
public class DatabaseImportCommand extends AbstractCommandImport {

	@Parameter(names = { "-catalog" }, description = "Catalog name")
	private String catalog;

	@Parameter(names = { "-schema" }, description = "Schema name")
	private String schema;

	@Parameter(description = "The list of tables to import")
	private List<String> tableNames = new ArrayList<String>();

	@Parameter(names = { "-fetch-size" }, description = "Fetch size")
	private Integer fetchSize;

	@Parameter(names = { "-sql" }, description = "SQL query")
	private String sql;

	@Parameter(names = { "-type" }, description = "Table type", converter = TableTypeConverter.class, validateWith = TableTypeConverter.class)
	private TableType tableType;

	private DatabaseApplication application;

	public DatabaseImportCommand(DatabaseApplication application) {
		this.application = application;
	}

	@Override
	protected Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace) {
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
