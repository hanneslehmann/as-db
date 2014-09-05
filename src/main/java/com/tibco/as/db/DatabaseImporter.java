package com.tibco.as.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
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
import com.tibco.as.io.Import;
import com.tibco.as.io.Importer;
import com.tibco.as.io.Transfer;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class DatabaseImporter extends Importer<Object[]> {

	private static final String SELECT = "SELECT {0} FROM {1}";

	private ConverterFactory converterFactory = new ConverterFactory();
	private Database database;
	private Connection connection;
	private Map<Table, ResultSet> resultSets = new HashMap<Table, ResultSet>();

	public DatabaseImporter(Metaspace metaspace, Database database) {
		super(metaspace);
		this.database = database;
	}

	@Override
	protected boolean isParallelTransfers() {
		return true;
	}

	@Override
	protected Import createTransfer() {
		return new DatabaseImport();
	}

	@Override
	protected Collection<Transfer> getTransfers(Metaspace metaspace)
			throws TransferException {
		Collection<Transfer> imports = new ArrayList<Transfer>();
		Collection<Table> tables = new ArrayList<Table>();
		for (Table table : getTables(database.getTables())) {
			try {
				tables.addAll(DatabaseCommon.getTables(connection, table));
			} catch (SQLException e) {
				throw new TransferException("Could not get tables", e);
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
	public SpaceDef getSpaceDef(Transfer transfer) throws Exception {
		DatabaseImport config = (DatabaseImport) transfer;
		Table table = config.getTable();
		if (table.getSql() == null) {
			String name = DatabaseCommon.getFullTableName(table);
			String[] columnNames = DatabaseCommon.getColumnNames(table);
			String names = DatabaseCommon.getCommaSeparated(columnNames);
			String sql = MessageFormat.format(SELECT, names, name);
			table.setSql(sql);
		}
		if (!resultSets.containsKey(table)) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(table.getSql());
			// resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
			if (table.getFetchSize() != null) {
				resultSet.setFetchSize(table.getFetchSize());
			}
			resultSets.put(table, resultSet);
		}
		ResultSet resultSet = resultSets.get(table);
		ResultSetMetaData metaData = resultSet.getMetaData();
		if (table.getColumns().isEmpty()) {
			for (int index = 0; index < metaData.getColumnCount(); index++) {
				Column column = new Column();
				column.setName(metaData.getColumnLabel(index + 1));
				table.getColumns().add(column);
			}
		}
		return super.getSpaceDef(transfer);
	}

	@Override
	protected void populateSpaceDef(SpaceDef spaceDef, Import transfer)
			throws SQLException {
		DatabaseImport config = (DatabaseImport) transfer;
		String[] keys = new String[0];
		for (Column column : config.getTable().getColumns()) {
			String fieldName = column.getField();
			FieldType fieldType = getFieldType(column);
			FieldDef fieldDef = FieldDef.create(fieldName, fieldType);
			fieldDef.setNullable(Boolean.TRUE.equals(column.isNullable()));
			if (column.getKeySequence() != null) {
				if (keys.length < column.getKeySequence()) {
					keys = Arrays.copyOf(keys, column.getKeySequence());
				}
				keys[column.getKeySequence() - 1] = fieldName;
			}
			spaceDef.getFieldDefs().add(fieldDef);
		}
		spaceDef.setKey(keys);
	}

	private FieldType getFieldType(Column column) {
		switch (column.getType()) {
		case BIGINT:
			return FieldType.LONG;
		case BIT:
			return FieldType.BOOLEAN;
		case BLOB:
			return FieldType.BLOB;
		case BOOLEAN:
			return FieldType.BOOLEAN;
		case CHAR:
			return getStringFieldType(column);
		case CLOB:
			return getStringFieldType(column);
		case DATE:
			return FieldType.DATETIME;
		case DECIMAL:
			return getNumericalFieldType(column);
		case DOUBLE:
			return FieldType.DOUBLE;
		case FLOAT:
			return FieldType.FLOAT;
		case INTEGER:
			return FieldType.INTEGER;
		case LONGNVARCHAR:
			return getStringFieldType(column);
		case LONGVARBINARY:
			return FieldType.BLOB;
		case LONGVARCHAR:
			return getStringFieldType(column);
		case NCHAR:
			return getStringFieldType(column);
		case NCLOB:
			return getStringFieldType(column);
		case NUMERIC:
			return getNumericalFieldType(column);
		case NVARCHAR:
			return getStringFieldType(column);
		case REAL:
			return getNumericalFieldType(column);
		case SMALLINT:
			return FieldType.SHORT;
		case SQLXML:
			return getStringFieldType(column);
		case TIME:
			return FieldType.DATETIME;
		case TIMESTAMP:
			return FieldType.DATETIME;
		case TINYINT:
			return FieldType.SHORT;
		case VARBINARY:
			return FieldType.BLOB;
		case VARCHAR:
			return getStringFieldType(column);
		default:
			return FieldType.STRING;
		}
	}

	private FieldType getStringFieldType(Column column) {
		if (getInt(column.getSize()) == 1) {
			return FieldType.CHAR;
		}
		return FieldType.STRING;
	}

	private FieldType getNumericalFieldType(Column column) {
		int decimalDigits = getInt(column.getDecimalDigits());
		int size = getInt(column.getSize());
		if (decimalDigits == 0) {
			if (size > AccessorFactory.INTEGER_SIZE) {
				return FieldType.LONG;
			}
			if (size > AccessorFactory.SHORT_SIZE) {
				return FieldType.INTEGER;
			}
			return FieldType.SHORT;
		}
		if (size > AccessorFactory.FLOAT_SIZE) {
			return FieldType.DOUBLE;
		}
		return FieldType.FLOAT;
	}

	private int getInt(Integer integer) {
		if (integer == null) {
			return 0;
		}
		return integer;
	}

	@Override
	protected String getInputSpaceName(Import config) {
		return ((DatabaseImport) config).getSpaceName();
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected IConverter<Object[], Tuple> getConverter(Transfer transfer,
			SpaceDef spaceDef) throws UnsupportedConversionException {
		DatabaseImport config = (DatabaseImport) transfer;
		List<Column> columns = config.getTable().getColumns();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			Column column = columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getField());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(), DatabaseCommon.getType(column),
					fieldDef);
		}
		return new ArrayToTupleConverter<Object>(accessors, converters);
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
		try {
			connection.close();
		} catch (SQLException e) {
			throw new TransferException("Could not close connection", e);
		}
	}

	@Override
	protected DatabaseInputStream getInputStream(Metaspace metaspace,
			Transfer transfer, SpaceDef spaceDef) throws TransferException {
		DatabaseImport config = (DatabaseImport) transfer;
		return new DatabaseInputStream(resultSets.get(config.getTable()));
	}
}
