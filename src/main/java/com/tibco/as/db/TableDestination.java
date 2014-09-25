package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.io.AbstractDestination;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.space.ASException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TableDestination extends AbstractDestination<Object[]> {

	private ConverterFactory converterFactory = new ConverterFactory();

	private DatabaseConnection connection;

	public TableDestination(DatabaseChannel channel, TableConfig config,
			DatabaseConnection connection) {
		super(channel, config);
		this.connection = connection;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected IConverter<Tuple, Object[]> getExportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		TableConfig export = (TableConfig) config;
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
	protected IOutputStream<Object[]> getOutputStream(DestinationConfig config,
			SpaceDef spaceDef) throws ASException, SQLException {
		Table table = ((TableConfig) config).getTable();
		if (table.getCatalog() == null && table.getSchema() == null) {
			table.setSchema(getChannel().getMetaspace().getName());
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
		List<Table> tables = connection.getTables(table);
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
			String fieldName = getFieldName(column);
			FieldDef fieldDef = spaceDef.getFieldDef(fieldName);
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
				int keyIndex = keyFieldNames.indexOf(fieldName);
				if (keyIndex != -1) {
					column.setKeySequence((short) (keyIndex + 1));
				}
			}
		}
		if (tables.isEmpty()) {
			connection.create(table);
		}
		PreparedStatement statement = connection.getInsertStatement(table);
		IPreparedStatementAccessor[] accessors = connection.getAccessors(table);
		return new TableOutputStream(statement, accessors);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected IConverter<Object[], Tuple> getImportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		List<Column> columns = ((TableConfig) config).getTable().getColumns();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			Column column = columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(getFieldName(column));
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(),
					connection.getType(column.getType()), fieldDef);
		}
		return new ArrayToTupleConverter<Object>(accessors, converters);
	}

	@Override
	protected IInputStream<Object[]> getInputStream(DestinationConfig config)
			throws Exception {
		Table table = ((TableConfig) config).getTable();
		if (table.getColumns().isEmpty() && table.getSelectSQL() == null) {
			table.getColumns().addAll(connection.getColumns(table));
		}
		IPreparedStatementAccessor[] accessors = connection.getAccessors(table);
		long count = connection.getCount(table);
		ResultSet resultSet = connection.select(table);
		for (Column column : connection.getColumns(resultSet)) {
			Column tableColumn = getColumn(table, column.getName());
			if (tableColumn == null) {
				table.getColumns().add(column);
			} else {
				tableColumn.setDecimalDigits(column.getDecimalDigits());
				tableColumn.setNullable(column.isNullable());
				tableColumn.setSize(column.getSize());
				tableColumn.setType(column.getType());
			}
		}
		return new TableInputStream(resultSet, accessors, count);
	}

	private Column getColumn(Table table, String columnName) {
		for (Column column : table.getColumns()) {
			if (columnName.equals(column.getName())) {
				return column;
			}
		}
		return null;
	}

	@Override
	protected void populateSpaceDef(SpaceDef spaceDef, DestinationConfig config)
			throws SQLException, UnsupportedJDBCTypeException {
		String[] keys = new String[0];
		for (Column column : ((TableConfig) config).getTable().getColumns()) {
			String fieldName = getFieldName(column);
			FieldType fieldType = connection.getFieldType(column.getType());
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

	private String getFieldName(Column column) {
		if (column.getField() == null) {
			return column.getName();
		}
		return column.getField();
	}
}
