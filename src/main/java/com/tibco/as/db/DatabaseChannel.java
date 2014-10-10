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
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.IDestination;
import com.tibco.as.log.LogFactory;

public class DatabaseChannel extends AbstractChannel {

	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String TABLE_CAT = "TABLE_CAT";
	private static final String TABLE_SCHEM = "TABLE_SCHEM";
	private static final String TABLE_TYPE = "TABLE_TYPE";

	private Logger log = LogFactory.getLog(DatabaseChannel.class);
	private DatabaseConfig config;
	private Connection connection;

	public DatabaseChannel(DatabaseConfig config) {
		super(config);
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		String url = config.getURL();
		log.log(Level.INFO, "Opening connection to database {0}", url);
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
	public void close() throws Exception {
		super.close();
		if (connection == null) {
			return;
		}
		log.info("Closing database connection");
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
	protected Collection<DestinationConfig> getImportConfigs(
			DestinationConfig config) throws Exception {
		Collection<DestinationConfig> configs = new ArrayList<DestinationConfig>();
		TableConfig table = (TableConfig) config;
		ResultSet resultSet = getTables(table);
		try {
			while (resultSet.next()) {
				TableConfig found = table.clone();
				found.setCatalog(resultSet.getString(TABLE_CAT));
				found.setTable(resultSet.getString(TABLE_NAME));
				found.setSchema(resultSet.getString(TABLE_SCHEM));
				found.setType(TableType.valueOf(resultSet.getString(TABLE_TYPE)));
				configs.add(found);
			}
		} finally {
			resultSet.close();
		}
		return configs;
	}

	public ResultSet getTables(TableConfig table) throws SQLException {
		String catalog = table.getCatalog();
		String schema = table.getSchema();
		String name = table.getTable();
		String[] types = { table.getType().name() };
		log.log(Level.FINE,
				"Retrieving table descriptions matching catalog={0} schema={1} name={2} types={3}",
				new Object[] { catalog, schema, name, Arrays.toString(types) });
		return getMetaData().getTables(catalog, schema, name, types);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		log.log(Level.FINE, "Preparing statement: {0}", sql);
		return connection.prepareStatement(sql);
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
}
