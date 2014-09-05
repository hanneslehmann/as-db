package com.tibco.as.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.io.Export;
import com.tibco.as.io.Exporter;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.io.Transfer;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class DatabaseExporter extends Exporter<Object[]> {

	private ConverterFactory converterFactory = new ConverterFactory();

	private Database database;

	private Connection connection;

	private boolean doNotCloseConnection;

	public DatabaseExporter(Metaspace metaspace, Database database) {
		super(metaspace);
		this.database = database;
	}

	@Override
	protected Collection<Transfer> getTransfers(Metaspace metaspace) {
		if (database.getTables().isEmpty()) {
			return super.getTransfers(metaspace);
		}
		Collection<Transfer> exports = new ArrayList<Transfer>();
		for (Table table : database.getTables()) {
			DatabaseExport export = (DatabaseExport) getDefaultTransfer();
			export.setTable(table);
			exports.add(export);
		}
		return exports;
	}

	@Override
	protected Export createTransfer() {
		return new DatabaseExport();
	}

	@Override
	public void execute() throws TransferException {
		try {
			connection = DatabaseCommon.getConnection(database);
		} catch (ClassNotFoundException e) {
			String message = MessageFormat.format(
					"Could not find driver ''{0}''", database.getDriver());
			throw new TransferException(message, e);
		} catch (SQLException e) {
			throw new TransferException("Could not connect to database", e);
		}
		super.execute();
		if (doNotCloseConnection) {
			return;
		}
		try {
			connection.close();
		} catch (SQLException e) {
			throw new TransferException("Could not close connection", e);
		}
	}

	public boolean isDoNotCloseConnection() {
		return doNotCloseConnection;
	}

	public void setDoNotCloseConnection(boolean doNotCloseConnection) {
		this.doNotCloseConnection = doNotCloseConnection;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected IConverter<Tuple, Object[]> getConverter(Transfer transfer,
			SpaceDef spaceDef) throws UnsupportedConversionException {
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
					DatabaseCommon.getType(column));
		}
		return new TupleToArrayConverter<Object>(accessors, converters,
				Object.class);
	}

	private int getSQLType(FieldType type) {
		switch (type) {
		case BLOB:
			return Types.BLOB;
		case BOOLEAN:
			return Types.BOOLEAN;
		case CHAR:
			return Types.CHAR;
		case DATETIME:
			return Types.TIMESTAMP;
		case DOUBLE:
			return Types.DOUBLE;
		case FLOAT:
			return Types.FLOAT;
		case INTEGER:
			return Types.INTEGER;
		case LONG:
			return Types.BIGINT;
		case SHORT:
			return Types.SMALLINT;
		default:
			return Types.VARCHAR;
		}
	}

	@Override
	protected IOutputStream<Object[]> getOutputStream(Metaspace metaspace,
			Transfer transfer, SpaceDef spaceDef) throws TransferException {
		DatabaseExport export = (DatabaseExport) transfer;
		if (export.getTable() == null) {
			export.setTable(new Table());
		}
		Table table = export.getTable();
		if (table.getCatalog() == null && table.getSchema() == null) {
			table.setSchema(metaspace.getName());
		}
		if (table.getName() == null) {
			table.setName(export.getSpaceName());
		}
		if (table.getSpace() == null) {
			table.setSpace(export.getSpaceName());
		}
		Map<String, Table> tables;
		try {
			tables = DatabaseCommon.getTableMap(connection, table);
		} catch (SQLException e) {
			throw new TransferException("Could not get tables", e);
		}
		String fullTableName = DatabaseCommon.getFullTableName(table);
		boolean exists = tables.containsKey(fullTableName);
		if (exists) {
			Table existingTable = tables.get(fullTableName);
			if (table.getColumns().isEmpty()) {
				table.getColumns().addAll(existingTable.getColumns());
			} else {
				for (Column column : table.getColumns()) {
					Column existingColumn = getColumn(existingTable, column);
					if (existingColumn == null) {
						continue;
					}
					if (column.getDecimalDigits() == null) {
						column.setDecimalDigits(existingColumn
								.getDecimalDigits());
					}
					if (column.getKeySequence() == null) {
						column.setKeySequence(existingColumn.getKeySequence());
					}
					if (column.getName() == null) {
						column.setName(existingColumn.getName());
					}
					if (column.getRadix() == null) {
						column.setRadix(existingColumn.getRadix());
					}
					if (column.getSize() == null) {
						column.setRadix(existingColumn.getRadix());
					}
					if (column.getType() == null) {
						column.setType(existingColumn.getType());
					}
					if (column.isNullable() == null) {
						column.setNullable(existingColumn.isNullable());
					}
				}
			}
		}
		if (table.getColumns().isEmpty()) {
			for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
				Column column = new Column();
				column.setField(fieldDef.getName());
				table.getColumns().add(column);
			}
		}
		List<String> keyFieldNames = new ArrayList<String>(spaceDef.getKeyDef()
				.getFieldNames());
		for (Column column : table.getColumns()) {
			String fieldName = column.getField();
			FieldDef fieldDef = spaceDef.getFieldDef(fieldName);
			if (column.getName() == null) {
				column.setName(fieldName);
			}
			if (column.getType() == null) {
				column.setType(JDBCType.valueOf(getSQLType(fieldDef.getType())));
			}
			if (column.isNullable() == null) {
				column.setNullable(fieldDef.isNullable());
			}
			if (column.getKeySequence() == null) {
				int index = keyFieldNames.indexOf(fieldName);
				if (index != -1) {
					column.setKeySequence((short) (index + 1));
				}
			}
		}
		if (!exists) {
			Statement statement;
			try {
				statement = connection.createStatement();
			} catch (SQLException e) {
				throw new TransferException("Could not create table", e);
			}
			String catalog = table.getCatalog();
			if (catalog != null) {
				try {
					if (!DatabaseCommon.getCatalogs(connection).contains(
							catalog)) {
						statement.execute("CREATE CATALOG " + catalog);
					}
				} catch (SQLException e) {
					throw new TransferException("Could not create catalog", e);
				}
			}
			String schema = table.getSchema();
			if (schema != null) {
				try {
					if (!DatabaseCommon.getSchemas(connection, catalog)
							.contains(schema)) {
						statement.execute("CREATE SCHEMA " + schema);
					}
				} catch (SQLException e) {
					throw new TransferException("Could not create schema", e);
				}
			}
			String sql = getCreateSQL(table);
			try {
				statement.execute(sql);
			} catch (SQLException e) {
				throw new TransferException("Could not create table", e);
			}
			try {
				statement.close();
			} catch (SQLException e) {
				throw new TransferException("Could not close statement", e);
			}
		}
		PreparedStatement statement;
		try {
			statement = connection.prepareStatement(getInsertSQL(export));
		} catch (SQLException e) {
			throw new TransferException("Could not prepare statement", e);
		}
		return new DatabaseOutputStream(statement);
	}

	private String getCreateSQL(Table table) {
		String query = "";
		query += "CREATE TABLE " + DatabaseCommon.getFullTableName(table)
				+ " (";
		String[] keys = new String[0];
		for (Column column : table.getColumns()) {
			query += column.getName() + " " + column.getType().name();
			if (!Boolean.TRUE.equals(column.isNullable())) {
				query += " not";
			}
			query += " null, ";
			Short keySequence = column.getKeySequence();
			if (keySequence != null) {
				if (keys.length < keySequence) {
					keys = Arrays.copyOf(keys, keySequence);
				}
				keys[keySequence - 1] = column.getName();
			}
		}
		query += "Primary Key (";
		for (int index = 0; index < keys.length; index++) {
			if (index > 0) {
				query += ",";
			}
			query += keys[index];
		}
		query += ")"; // close primary key constraint
		query += ")"; // close table def
		return query;
	}

	private Column getColumn(Table existing, Column column) {
		for (Column existingColumn : existing.getColumns()) {
			if (existingColumn.getName().equals(column.getName())) {
				return existingColumn;
			}
		}
		return null;
	}

	private String getInsertSQL(DatabaseExport export) {
		String tableName = DatabaseCommon.getFullTableName(export.getTable());
		String[] columnNames = DatabaseCommon.getColumnNames(export.getTable());
		String[] questionMarks = new String[columnNames.length];
		Arrays.fill(questionMarks, "?");
		return MessageFormat.format("INSERT INTO {0} ({1}) VALUES ({2})",
				tableName, DatabaseCommon.getCommaSeparated(columnNames),
				DatabaseCommon.getCommaSeparated(questionMarks));
	}
}
