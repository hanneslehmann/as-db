package com.tibco.as.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXB;

import com.tibco.as.io.Channel;
import com.tibco.as.io.Destination;
import com.tibco.as.log.LogFactory;

public class DatabaseChannel extends Channel {

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
	private String configPath;
	private String driver;
	private String jar;
	private String url;
	private String user;
	private String password;
	private Connection connection;

	public DatabaseChannel(String metaspaceName) {
		super(metaspaceName);
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getJar() {
		return jar;
	}

	public void setJar(String jar) {
		this.jar = jar;
	}

	public String getURL() {
		return url;
	}

	public void setURL(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public void open() throws Exception {
		loadConfig();
		log.info("Connecting to database");
		Properties props = new Properties();
		if (user != null) {
			props.put("user", user);
		}
		if (password != null) {
			props.put("password", password);
		}
		if (driver == null) {
			connection = DriverManager.getConnection(url, props);
		} else {
			Driver driver = getDriverClass().newInstance();
			connection = driver.connect(url, props);
		}
		log.finest("Setting the connection's commit mode to auto");
		connection.setAutoCommit(true);
		super.open();
	}

	private void loadConfig() throws FileNotFoundException {
		if (configPath == null) {
			return;
		}
		FileInputStream in = new FileInputStream(configPath);
		Database database = JAXB.unmarshal(in, Database.class);
		for (Table table : database.getTables()) {
			TableDestination destination = newDestination();
			destination.setCatalog(table.getCatalog());
			destination.setCountSQL(table.getCountSQL());
			destination.setInsertSQL(table.getInsertSQL());
			destination.setTable(table.getName());
			destination.setSchema(table.getSchema());
			destination.setSelectSQL(table.getSelectSQL());
			destination.setSpaceName(table.getSpace());
			destination.setType(table.getType());
			for (Column column : table.getColumns()) {
				ColumnConfig columnConfig = new ColumnConfig();
				columnConfig.setFieldName(column.getField());
				columnConfig.setColumnName(column.getName());
				columnConfig.setColumnNullable(column.isNullable());
				columnConfig.setColumnSize(column.getSize());
				columnConfig.setColumnType(column.getType());
				columnConfig.setDecimalDigits(column.getDecimals());
				columnConfig.setKeySequence(column.getKeySequence());
				columnConfig.setRadix(column.getRadix());
				destination.getColumns().add(columnConfig);
			}
			addDestination(destination);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	@SuppressWarnings("unchecked")
	private Class<Driver> getDriverClass() throws MalformedURLException,
			ClassNotFoundException {
		if (jar == null) {
			return (Class<Driver>) Class.forName(driver);
		}
		URL[] urls = { new URL("file://" + jar) };
		URLClassLoader classLoader = URLClassLoader.newInstance(urls);
		log.log(Level.FINE, "Loading driver ''{0}'' from {1}", new Object[] {
				driver, Arrays.toString(urls) });
		return (Class<Driver>) classLoader.loadClass(driver);
	}

	@Override
	public void close() throws Exception {
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

	@Override
	public TableDestination newDestination() {
		return new TableDestination(this);
	}

	@Override
	protected Collection<Destination> getImportDestinations(
			Destination destination) throws SQLException {
		Collection<Destination> destinations = new ArrayList<Destination>();
		destinations.addAll(getTables((TableDestination) destination));
		return destinations;
	}

	public Collection<TableDestination> getTables(TableDestination table)
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
		Collection<TableDestination> tables = new ArrayList<TableDestination>();
		try {
			while (resultSet.next()) {
				TableDestination found = table.clone();
				found.setCatalog(resultSet.getString(TABLE_CAT));
				found.setTable(resultSet.getString(TABLE_NAME));
				found.setSchema(resultSet.getString(TABLE_SCHEM));
				found.setType(TableType.valueOf(resultSet.getString(TABLE_TYPE)));
				tables.add(found);
			}
		} finally {
			resultSet.close();
		}
		for (TableDestination destination : tables) {
			Collection<ColumnConfig> columns = new ArrayList<ColumnConfig>();
			for (ColumnConfig column : getColumns(destination)) {
				ResultSet columnRS = getMetaData().getColumns(
						destination.getCatalog(), destination.getSchema(),
						destination.getTable(), column.getColumnName());
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
			destination.setColumns(columns);
			ResultSet keyRS = getMetaData().getPrimaryKeys(
					destination.getCatalog(), destination.getSchema(),
					destination.getTable());
			try {
				while (keyRS.next()) {
					String columnName = keyRS.getString(COLUMN_NAME);
					ColumnConfig column = destination.getColumn(columnName);
					if (column == null) {
						continue;
					}
					column.setKeySequence(keyRS.getShort(KEY_SEQ));
				}
			} finally {
				keyRS.close();
			}
		}
		return tables;
	}

	private Collection<ColumnConfig> getColumns(TableDestination destination) {
		if (destination.getColumns().isEmpty()) {
			return Arrays.asList(new ColumnConfig());
		}
		return destination.getColumns();
	}

	public void setTableNames(Collection<String> tableNames) {
		for (String tableName : tableNames) {
			TableDestination destination = newDestination();
			destination.setTable(tableName);
			addDestination(destination);
		}

	}

}
