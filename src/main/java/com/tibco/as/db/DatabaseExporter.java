package com.tibco.as.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.io.AbstractExporter;
import com.tibco.as.io.AbstractTransfer;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class DatabaseExporter extends AbstractExporter<Object[]> {

	private ConverterFactory converterFactory = new ConverterFactory();

	private Database database;

	private boolean keepConnectionOpen;

	private DatabaseConnection connection;

	public DatabaseExporter(Metaspace metaspace, Database database) {
		super(metaspace);
		this.database = database;
	}

	@Override
	protected Collection<AbstractTransfer> getTransfers(Metaspace metaspace) {
		if (database.getTables().isEmpty()) {
			return super.getTransfers(metaspace);
		}
		Collection<AbstractTransfer> exports = new ArrayList<AbstractTransfer>();
		for (Table table : database.getTables()) {
			DatabaseExport export = (DatabaseExport) getDefaultTransfer();
			export.setTable(table);
			exports.add(export);
		}
		return exports;
	}

	@Override
	protected DatabaseExport createTransfer() {
		return new DatabaseExport();
	}

	@Override
	public void execute() throws TransferException {
		connection = new DatabaseConnection(database);
		try {
			connection.open();
		} catch (Exception e) {
			throw new TransferException("Could not connect to database", e);
		}
		super.execute();
		if (keepConnectionOpen) {
			return;
		}
		try {
			close();
		} catch (SQLException e) {
			throw new TransferException("Could not close connection", e);
		}
	}

	public void close() throws SQLException {
		connection.close();
	}

	public boolean isKeepConnectionOpen() {
		return keepConnectionOpen;
	}

	public void setKeepConnectionOpen(boolean keepConnectionOpen) {
		this.keepConnectionOpen = keepConnectionOpen;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected IConverter<Tuple, Object[]> getConverter(
			AbstractTransfer transfer, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		DatabaseExport export = (DatabaseExport) transfer;
		List<Column> columns = export.getTable().getColumns();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			Column column = columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getField());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					export.getAttributes(), fieldDef,
					connection.getType(column.getType()));
		}
		return new TupleToArrayConverter<Object>(accessors, converters,
				Object.class);
	}

	@Override
	protected IOutputStream<Object[]> getOutputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef)
			throws TransferException {
		DatabaseExport export = (DatabaseExport) transfer;
		Table table = export.getTable();
		if (table.getCatalog() == null && table.getSchema() == null) {
			table.setSchema(metaspace.getName());
		}
		if (table.getSpace() == null) {
			table.setSpace(spaceDef.getName());
		}
		if (table.getName() == null) {
			table.setName(table.getSpace());
		}
		if (table.getColumns().isEmpty()) {
			for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
				Column column = new Column();
				column.setField(fieldDef.getName());
				table.getColumns().add(column);
			}
		}
		List<Table> tables;
		try {
			tables = connection.getTables(table);
		} catch (SQLException e) {
			throw new TransferException("Could not get tables", e);
		}
		if (!tables.isEmpty()) {
			Table existing = tables.get(0);
			table.setCatalog(existing.getCatalog());
			table.setName(existing.getName());
			table.setSchema(existing.getSchema());
			table.setType(existing.getType());
			table.getColumns().clear();
			table.getColumns().addAll(existing.getColumns());
		}
		List<String> keyFieldNames = new ArrayList<String>(spaceDef.getKeyDef()
				.getFieldNames());
		for (Column column : table.getColumns()) {
			if (column.getField() == null) {
				column.setField(column.getName());
			}
			if (column.getName() == null) {
				column.setName(column.getField());
			}
			FieldDef fieldDef = spaceDef.getFieldDef(column.getField());
			if (fieldDef == null) {
				continue;
			}
			if (column.getType() == null) {
				column.setType(connection.getColumnType(fieldDef.getType()));
			}
			if (column.getSize() == null) {
				column.setSize(connection.getSize(fieldDef.getType()));
			}
			if (column.isNullable() == null) {
				column.setNullable(fieldDef.isNullable());
			}
			if (column.getKeySequence() == null) {
				int keyIndex = keyFieldNames.indexOf(column.getField());
				if (keyIndex != -1) {
					column.setKeySequence((short) (keyIndex + 1));
				}
			}
		}
		if (tables.isEmpty()) {
			try {
				connection.create(table);
			} catch (SQLException e) {
				throw new TransferException("Could not create table", e);
			}
		}
		PreparedStatement statement;
		try {
			statement = connection.getInsertStatement(table);
		} catch (SQLException e) {
			throw new TransferException("Could not prepare statement", e);
		}
		IPreparedStatementAccessor[] accessors = connection.getAccessors(table);
		return new OutputStream(statement, accessors);
	}

	public Connection getConnection() {
		return connection.getConnection();
	}

}