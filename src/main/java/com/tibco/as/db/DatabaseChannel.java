package com.tibco.as.db;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.Field;
import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.IDestination;
import com.tibco.as.log.LogFactory;

public class DatabaseChannel extends AbstractChannel {

	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String TABLE_CAT = "TABLE_CAT";
	private static final String TABLE_SCHEM = "TABLE_SCHEM";
	private static final String TABLE_TYPE = "TABLE_TYPE";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String COLUMN_DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private static final String KEY_SEQ = "KEY_SEQ";
	private static final String NULLABLE = "NULLABLE";
	private static final String COLUMN_NUM_PREC_RADIX = "NUM_PREC_RADIX";
	private static final String COLUMN_ORDINAL_POSITION = "ORDINAL_POSITION";

	private Logger log = LogFactory.getLog(DatabaseChannel.class);
	private DatabaseConfig config;
	private Connection connection;

	public DatabaseChannel(DatabaseConfig config) {
		super(config);
		this.config = config;
	}

	@Override
	protected void open() throws Exception {
		String url = config.getURL();
		log.info("Connecting to database");
		Properties props = new Properties();
		if (config.getUser() != null) {
			props.put("user", config.getUser());
		}
		if (config.getPassword() != null) {
			props.put("password", config.getPassword());
		}
		if (config.getDriver() == null) {
			connection = DriverManager.getConnection(url, props);
		} else {
			Driver driver = getDriverClass().newInstance();
			connection = driver.connect(url, props);
		}
		log.finest("Setting the connection's commit mode to auto");
		connection.setAutoCommit(true);
		super.open();
	}

	@SuppressWarnings("unchecked")
	private Class<Driver> getDriverClass() throws MalformedURLException,
			ClassNotFoundException {
		if (config.getJar() == null) {
			return (Class<Driver>) Class.forName(config.getDriver());
		}
		URL[] urls = { new URL("file://" + config.getJar()) };
		URLClassLoader classLoader = URLClassLoader.newInstance(urls);
		log.log(Level.FINE, "Loading driver ''{0}'' from {1}", new Object[] {
				config.getDriver(), Arrays.toString(urls) });
		return (Class<Driver>) classLoader.loadClass(config.getDriver());
	}

	@Override
	protected void close() throws Exception {
		super.close();
		if (connection == null) {
			return;
		}
		log.info("Disconnecting from database");
		connection.close();
		connection = null;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		Statement statement = connection.createStatement();
		log.log(Level.FINE, "Executing query: {0}", sql);
		try {
			return statement.executeQuery(sql);
		} catch (SQLException e) {
			statement.close();
			throw e;
		}
	}

	@Override
	protected IDestination createDestination(DestinationConfig config) {
		return new TableDestination(this, (TableConfig) config);
	}

	@Override
	protected void discover() throws Exception {
		for (DestinationConfig destination : config.getDestinations()) {
			if (destination.isImport()) {
				config.removeDestinationConfig(destination);
				for (TableConfig table : getTables((TableConfig) destination)) {
					config.addDestinationConfig(table);
				}
			}
		}
		super.discover();
	}

	private Collection<Field> getTableColumns(TableConfig config) {
		if (config.getFields().isEmpty()) {
			config.addField();
		}
		return config.getFields();
	}

	public Collection<TableConfig> getTables(TableConfig table)
			throws SQLException {
		String catalog = table.getCatalog();
		String schema = table.getSchema();
		String name = table.getTable();
		String[] types = { table.getType().name() };
		log.log(Level.FINE,
				"Retrieving table descriptions matching catalog={0} schema={1} name={2} types={3}",
				new Object[] { catalog, schema, name, Arrays.toString(types) });
		ResultSet resultSet = getMetaData().getTables(catalog, schema, name,
				types);
		Collection<TableConfig> tables = new ArrayList<TableConfig>();
		try {
			while (resultSet.next()) {
				TableConfig found = table.clone();
				found.setCatalog(resultSet.getString(TABLE_CAT));
				found.setTable(resultSet.getString(TABLE_NAME));
				found.setSchema(resultSet.getString(TABLE_SCHEM));
				found.setType(TableType.valueOf(resultSet.getString(TABLE_TYPE)));
				tables.add(found);
			}
		} finally {
			resultSet.close();
		}
		for (TableConfig config : tables) {
			Collection<Field> columns = new ArrayList<Field>();
			for (Field field : getTableColumns(config)) {
				ColumnConfig column = (ColumnConfig) field;
				ResultSet columnRS = getMetaData().getColumns(
						config.getCatalog(), config.getSchema(),
						config.getTable(), column.getColumnName());
				Map<Integer, ColumnConfig> columnMap = new TreeMap<Integer, ColumnConfig>();
				try {
					while (columnRS.next()) {
						ColumnConfig found = column.clone();
						found.setDecimalDigits(columnRS
								.getInt(COLUMN_DECIMAL_DIGITS));
						found.setColumnName(columnRS.getString(COLUMN_NAME));
						found.setColumnNullable(columnRS.getBoolean(NULLABLE));
						found.setRadix(columnRS.getInt(COLUMN_NUM_PREC_RADIX));
						found.setColumnSize(columnRS.getInt(COLUMN_SIZE));
						int dataType = columnRS.getInt(COLUMN_DATA_TYPE);
						found.setColumnType(JDBCType.valueOf(dataType));
						int ordinalPosition = columnRS
								.getInt(COLUMN_ORDINAL_POSITION);
						columnMap.put(ordinalPosition, found);
					}
				} finally {
					columnRS.close();
				}
				columns.addAll(columnMap.values());
			}
			config.setFields(columns);
			ResultSet keyRS = getMetaData().getPrimaryKeys(config.getCatalog(),
					config.getSchema(), config.getTable());
			try {
				while (keyRS.next()) {
					String columnName = keyRS.getString(COLUMN_NAME);
					ColumnConfig column = config.getColumn(columnName);
					column.setKeySequence(keyRS.getShort(KEY_SEQ));
				}
			} finally {
				keyRS.close();
			}
		}
		return tables;
	}

	public void execute(String sql) throws SQLException {
		Statement statement = connection.createStatement();
		log.log(Level.FINE, "Executing statement: {0}", sql);
		try {
			statement.execute(sql);
		} finally {
			statement.close();
		}
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
}
