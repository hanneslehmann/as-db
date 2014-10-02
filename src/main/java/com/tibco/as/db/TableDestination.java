package com.tibco.as.db;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.io.AbstractDestination;
import com.tibco.as.io.FieldConfig;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TableDestination extends AbstractDestination<Object[]> {

	private final static int DEFAULT_INSERT_BATCH_SIZE = 1000;

	private ConverterFactory converterFactory = new ConverterFactory();
	private DatabaseChannel channel;
	private TableConfig config;

	public TableDestination(DatabaseChannel channel, TableConfig config) {
		super(channel, config);
		this.channel = channel;
		this.config = config;
	}

	@Override
	protected int getImportBatchSize() {
		TableConfig tableConfig = (TableConfig) config;
		if (tableConfig.getInsertBatchSize() == null) {
			return DEFAULT_INSERT_BATCH_SIZE;
		}
		return tableConfig.getInsertBatchSize();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected IConverter<Tuple, Object[]> getExportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException {
		List<FieldConfig> columns = config.getFields();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			ColumnConfig column = (ColumnConfig) columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getFieldName());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(), fieldDef,
					getType(column.getColumnType()));
		}
		return new TupleToArrayConverter<Object>(accessors, converters,
				Object.class);
	}

	private Class<?> getType(JDBCType type) {
		switch (type) {
		case CHAR:
		case VARCHAR:
		case LONGVARCHAR:
			return String.class;
		case NUMERIC:
		case DECIMAL:
			return BigDecimal.class;
		case BIT:
		case BOOLEAN:
			return Boolean.class;
		case TINYINT:
		case SMALLINT:
			return Short.class;
		case INTEGER:
			return Integer.class;
		case BIGINT:
			return Long.class;
		case REAL:
			return Float.class;
		case FLOAT:
		case DOUBLE:
			return Double.class;
		case BINARY:
		case VARBINARY:
		case LONGVARBINARY:
			return byte[].class;
		case DATE:
			return Date.class;
		case TIME:
			return Time.class;
		case TIMESTAMP:
			return Timestamp.class;
		case CLOB:
			return Clob.class;
		case BLOB:
			return byte[].class;
		default:
			return Object.class;
		}
	}

	@Override
	protected IOutputStream<Object[]> getOutputStream() throws SQLException {
		if (channel.exists(config)) {
			channel.populate(config);
		} else {
			channel.create(config);
		}
		PreparedStatement statement = channel.getInsertStatement(config);
		IPreparedStatementAccessor[] accessors = channel.getAccessors(config);
		return new TableOutputStream(statement, accessors);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected IConverter<Object[], Tuple> getImportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException {
		List<FieldConfig> columns = config.getFields();
		ITupleAccessor[] accessors = new ITupleAccessor[columns.size()];
		IConverter[] converters = new IConverter[columns.size()];
		for (int index = 0; index < columns.size(); index++) {
			ColumnConfig column = (ColumnConfig) columns.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getFieldName());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(), getType(column.getColumnType()),
					fieldDef);
		}
		return new ArrayToTupleConverter<Object>(accessors, converters);
	}

	@Override
	protected IInputStream<Object[]> getInputStream() throws Exception {
		if (config.getFields().isEmpty() && config.getSelectSQL() == null) {
			channel.populate(config);
		}
		IPreparedStatementAccessor[] accessors = channel.getAccessors(config);
		long count = channel.getCount(config);
		ResultSet resultSet = channel.select(config);
		if (config.getFetchSize() != null) {
			resultSet.setFetchSize(config.getFetchSize());
		}
		ResultSetMetaData metaData = resultSet.getMetaData();
		for (int index = 0; index < metaData.getColumnCount(); index++) {
			int columnIndex = index + 1;
			ColumnConfig column = getColumn(config,
					metaData.getColumnLabel(columnIndex));
			column.setColumnSize(metaData.getPrecision(columnIndex));
			column.setDecimalDigits(metaData.getScale(columnIndex));
			column.setColumnNullable(metaData.isNullable(columnIndex) == ResultSetMetaData.columnNullable);
			column.setColumnType(JDBCType.valueOf(metaData
					.getColumnType(columnIndex)));
		}
		return new TableInputStream(resultSet, accessors, count);
	}

	private ColumnConfig getColumn(TableConfig table, String columnName) {
		for (FieldConfig field : table.getFields()) {
			ColumnConfig column = (ColumnConfig) field;
			if (columnName.equals(column.getColumnName())) {
				return column;
			}
		}
		ColumnConfig column = new ColumnConfig();
		column.setColumnName(columnName);
		table.getFields().add(column);
		return column;
	}

}
