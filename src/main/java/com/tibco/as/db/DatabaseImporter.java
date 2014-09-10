package com.tibco.as.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.io.AbstractImport;
import com.tibco.as.io.AbstractImporter;
import com.tibco.as.io.AbstractTransfer;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class DatabaseImporter extends AbstractImporter<Object[]> {

	private ConverterFactory converterFactory = new ConverterFactory();
	private Database database;
	private Map<Table, ResultSet> resultSets = new HashMap<Table, ResultSet>();

	private DatabaseConnection connection;

	public DatabaseImporter(Metaspace metaspace, Database database) {
		super(metaspace);
		this.database = database;
	}

	@Override
	protected boolean isParallelTransfers() {
		return true;
	}

	@Override
	protected DatabaseImport createTransfer() {
		return new DatabaseImport();
	}

	@Override
	protected Collection<AbstractTransfer> getTransfers(Metaspace metaspace)
			throws TransferException {
		Collection<AbstractTransfer> imports = new ArrayList<AbstractTransfer>();
		Collection<Table> tables = new ArrayList<Table>();
		for (Table table : getTables(database.getTables())) {
			if (table.getSql() == null) {
				try {
					tables.addAll(connection.getTables(table));
				} catch (SQLException e) {
					throw new TransferException("Could not get tables", e);
				}
			} else {
				tables.add(table);
			}
		}
		database.getTables().clear();
		database.getTables().addAll(tables);
		for (Table table : tables) {
			DatabaseImport config = (DatabaseImport) getDefaultTransfer()
					.clone();
			config.setTable(table);
			imports.add(config);
		}
		return imports;
	}

	private Collection<Table> getTables(Collection<Table> tables) {
		if (tables.isEmpty()) {
			return Arrays.asList(new Table());
		}
		return tables;
	}

	@Override
	public SpaceDef getSpaceDef(AbstractTransfer transfer) throws Exception {
		DatabaseImport config = (DatabaseImport) transfer;
		Table table = config.getTable();
		ResultSet resultSet = connection.select(table);
		resultSets.put(table, resultSet);
		Map<String, Column> columns = connection.getColumns(resultSet);
		if (table.getColumns().isEmpty()) {
			table.getColumns().addAll(columns.values());
		}
		for (Column column : table.getColumns()) {
			if (columns.containsKey(column.getName())) {
				Column existing = columns.get(column.getName());
				column.setDecimalDigits(existing.getDecimalDigits());
				column.setNullable(existing.isNullable());
				column.setSize(existing.getSize());
				column.setType(existing.getType());
				if (column.getField() == null) {
					column.setField(column.getName());
				}
			}
		}
		return super.getSpaceDef(transfer);
	}

	@Override
	protected void populateSpaceDef(SpaceDef spaceDef, AbstractImport transfer)
			throws SQLException {
		DatabaseImport config = (DatabaseImport) transfer;
		String[] keys = new String[0];
		for (Column column : config.getTable().getColumns()) {
			String fieldName = column.getField();
			FieldType fieldType = connection.getFieldType(column);
			FieldDef fieldDef = FieldDef.create(fieldName, fieldType);
			fieldDef.setNullable(Boolean.TRUE.equals(column.isNullable()));
			Short keySequence = column.getKeySequence();
			if (keySequence != null) {
				if (keys.length < keySequence) {
					keys = Arrays.copyOf(keys, keySequence);
				}
				keys[keySequence - 1] = fieldName;
			}
			spaceDef.getFieldDefs().add(fieldDef);
		}
		spaceDef.setKey(keys);
	}

	@Override
	protected String getInputSpaceName(AbstractImport config) {
		return ((DatabaseImport) config).getSpaceName();
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected IConverter<Object[], Tuple> getConverter(
			AbstractTransfer transfer, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		DatabaseImport config = (DatabaseImport) transfer;
		List<Column> columns = config.getTable().getColumns();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			Column column = columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getField());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(),
					connection.getType(column.getType()), fieldDef);
		}
		return new ArrayToTupleConverter<Object>(accessors, converters);
	}

	@Override
	public void execute() throws TransferException {
		try {
			connection = new DatabaseConnection(database);
		} catch (Exception e) {
			throw new TransferException("Could not connect to database", e);
		}
		super.execute();
		try {
			connection.close();
		} catch (SQLException e) {
			throw new TransferException("Could not close connection", e);
		}
	}

	@Override
	protected DatabaseInputStream getInputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef)
			throws TransferException {
		DatabaseImport config = (DatabaseImport) transfer;
		Table table = config.getTable();
		ResultSet resultSet = resultSets.get(table);
		IPreparedStatementAccessor[] accessors = connection.getAccessors(table);
		return new DatabaseInputStream(resultSet, accessors);
	}
}
