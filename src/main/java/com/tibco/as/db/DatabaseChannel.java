package com.tibco.as.db;

import java.io.File;
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
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXB;

import com.tibco.as.io.Channel;
import com.tibco.as.util.log.LogFactory;

public class DatabaseChannel extends Channel {

	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String TABLE_CAT = "TABLE_CAT";
	private static final String TABLE_SCHEM = "TABLE_SCHEM";
	private static final String TABLE_TYPE = "TABLE_TYPE";

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
		File configFile = new File(configPath);
		if (!configFile.exists()) {
			return;
		}
		Database database = JAXB.unmarshal(configFile, Database.class);
		for (Table table : database.getTables()) {
			getDestinations().add(new TableDestination(this, table));
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

	public void setTableNames(Collection<String> tableNames) {
		for (String tableName : tableNames) {
			Table table = new Table();
			table.setName(tableName);
			getDestinations().add(new TableDestination(this, table));
		}
	}

	public Collection<Table> getTables(String catalog, String schema,
			String name, TableType type) throws SQLException {
		String[] types = new String[] { getTableType(type).name() };
		log.log(Level.FINE,
				"Retrieving table descriptions matching catalog={0} schema={1} name={2} types={3}",
				new Object[] { catalog, schema, name, types });
		ResultSet rs = getMetaData().getTables(catalog, schema, name, types);
		Collection<Table> tables = new ArrayList<Table>();
		try {
			while (rs.next()) {
				Table table = new Table();
				table.setCatalog(rs.getString(TABLE_CAT));
				table.setName(rs.getString(TABLE_NAME));
				table.setSchema(rs.getString(TABLE_SCHEM));
				table.setType(TableType.valueOf(rs.getString(TABLE_TYPE)));
				tables.add(table);
			}
		} finally {
			rs.close();
		}
		return tables;
	}

	private TableType getTableType(TableType type) {
		if (type == null) {
			return TableType.TABLE;
		}
		return type;
	}

}
